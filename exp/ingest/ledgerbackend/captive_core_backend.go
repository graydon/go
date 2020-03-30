package ledgerbackend

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/historyarchive"
	"github.com/stellar/go/xdr"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"
)

// Ensure captiveStellarCore implements LedgerBackend
var _ LedgerBackend = (*captiveStellarCore)(nil)

// This is a not-very-complete or well-organized sketch of code be used to
// stream LedgerCloseMeta data from a "captive" stellar-core: one running as a
// subprocess and replaying portions of history against an in-memory ledger.
//
// A captive stellar-core still needs (and allocates, in os.TempDir()) a
// temporary directory to run in: one in which its config file is stored, along
// with temporary files it downloads and decompresses, and its bucket
// state. Only the ledger will be in-memory (and we might even switch this to
// SQLite + large buffers in the future if the in-memory ledger gets too big.)
//
// Feel free to reorganize this to fit better. It's preliminary!

// TODO: switch from history URLs to history archive interface provided from support package, to permit mocking

// In this (crude, initial) sketch, we replay ledgers in blocks of 100,000
const ledgersPerProcess = 100000
const ledgersPerCheckpoint = 64
const numLogLinesToKeep = 1024

// Circular buffer of recent log lines from captive stellar core
type recentLogLines struct {
	lines [numLogLinesToKeep]*string
	offset int
	lock sync.RWMutex
}

func (r *recentLogLines) addLogLine(line string) {
	r.lock.Lock()
	r.lines[r.offset] = &line
	r.offset = (r.offset + 1) % numLogLinesToKeep
	r.lock.Unlock()
}

func (r *recentLogLines) getLogLines() []string {
	r.lock.RLock()
	var tmp []string
	for i := 0; i < numLogLinesToKeep; i++ {
		n := r.lines[(r.offset+i)%numLogLinesToKeep]
		if n != nil {
			tmp = append(tmp, *n)
		}
	}
	r.lock.RUnlock()
	return tmp
}

func roundDownToCheckpointStart(ledger uint32) uint32 {
	v := (ledger / ledgersPerCheckpoint) * ledgersPerCheckpoint
	if v == 0 {
		// There's no ledger 0, the first checkpoint starts at 1
		return 1
	} else {
		// All other checkpoints start at the next multiple of 64
		return v
	}
}

type captiveStellarCore struct {
	nonce             string
	networkPassphrase string
	historyURLs       []string
	nextLedger        uint32
	lastFewLogLines   recentLogLines
	cmd               *exec.Cmd
	metaPipe          io.Reader
}

// Returns a new captiveStellarCore that is not running. Will lazily start a subprocess
// to feed it a block of streaming metadata when user calls .GetLedger(), and will kill
// and restart the subprocess if subsequent calls to .GetLedger() are discontiguous.
//
// Platform-specific pipe setup logic is in the .start() methods.
func NewCaptive(networkPassphrase string, historyURLs []string) *captiveStellarCore {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &captiveStellarCore{
		nonce:             fmt.Sprintf("captive-stellar-core-%x", r.Uint64()),
		networkPassphrase: networkPassphrase,
		historyURLs:       historyURLs,
		nextLedger:        0,
	}
}

func (c *captiveStellarCore) GetRecentLogLines() []string {
	return c.lastFewLogLines.getLogLines()
}

// XDR and RPC define a (minimal) framing format which our metadata arrives in: a 4-byte
// big-endian length header that has the high bit set, followed by that length worth of
// XDR data. Decoding this involves just a little more work than xdr.Unmarshal.
func unmarshalFramed(r io.Reader, v interface{}) (int, error) {
	var frameLen uint32
	n, e := xdr.Unmarshal(r, &frameLen)
	if e != nil {
		return n, errors.Wrap(e, "unmarshalling XDR frame header")
	}
	if n != 4 {
		return n, errors.New("bad length of XDR frame header")
	}
	if (frameLen & 0x80000000) != 0x80000000 {
		return n, errors.New("malformed XDR frame header")
	}
	frameLen &= 0x7fffffff
	m, e := xdr.Unmarshal(r, v)
	if e != nil {
		return n + m, errors.Wrap(e, "unmarshalling framed XDR")
	}
	if int64(m) != int64(frameLen) {
		return n + m, errors.New("bad length of XDR frame body")
	}
	return m + n, nil
}

func peekLedgerSequence(xlcm *xdr.LedgerCloseMeta) (uint32, error) {
	v0, ok := xlcm.GetV0()
	if !ok {
		return 0, errors.New("unexpected XDR LedgerCloseMeta version")
	}
	return uint32(v0.LedgerHeader.Header.LedgerSeq), nil
}

// Note: the xdr.LedgerCloseMeta structure is _not_ the same as
// the ledgerbackend.LedgerCloseMeta structure; the latter should
// probably migrate to the former eventually.
func (c *captiveStellarCore) copyLedgerCloseMeta(xlcm *xdr.LedgerCloseMeta, lcm *LedgerCloseMeta) error {
	v0, ok := xlcm.GetV0()
	if !ok {
		return errors.New("unexpected XDR LedgerCloseMeta version")
	}
	lcm.LedgerHeader = v0.LedgerHeader
	envelopes := make(map[xdr.Hash]xdr.TransactionEnvelope)
	for _, tx := range v0.TxSet.Txs {
		hash, e := network.HashTransaction(&tx.Tx, c.networkPassphrase)
		if e != nil {
			return errors.Wrap(e, "hashing tx in LedgerCloseMeta")
		}
		envelopes[hash] = tx
	}
	for _, trm := range v0.TxProcessing {
		txe, ok := envelopes[trm.Result.TransactionHash]
		if !ok {
			return errors.New("unknown tx hash in LedgerCloseMeta")
		}
		lcm.TransactionEnvelope = append(lcm.TransactionEnvelope, txe)
		lcm.TransactionResult = append(lcm.TransactionResult, trm.Result)
		lcm.TransactionMeta = append(lcm.TransactionMeta, trm.TxApplyProcessing)
		lcm.TransactionFeeChanges = append(lcm.TransactionFeeChanges, trm.FeeProcessing)
	}
	for _, urm := range v0.UpgradesProcessing {
		lcm.UpgradesMeta = append(lcm.UpgradesMeta, urm.Changes)
	}
	return nil
}

// We assume that we'll be called repeatedly asking for ledgers in ascending
// order, so when asked for ledger 23 we start a subprocess doing catchup
// "100023/100000", which should replay 23, 24, 25, ... 100023. The wrinkle in
// this is that core will actually replay from the _checkpoint before_
// the implicit start ledger, so we might need to skip a few ledgers until
// we hit the one requested (this routine does so transparently if needed).
func (c *captiveStellarCore) GetLedger(sequence uint32) (bool, LedgerCloseMeta, error) {
	if c.nextLedger != sequence {
		c.Close()
		maxLedger, e := c.GetLatestLedgerSequence()
		if e != nil {
			return false, LedgerCloseMeta{}, errors.Wrap(e, "getting latest ledger sequence")
		}
		if sequence > maxLedger {
			return false, LedgerCloseMeta{}, errors.Errorf("sequence %d greater than max available %d",
				sequence, maxLedger)
		}
		lastLedger := sequence + ledgersPerProcess
		if lastLedger > maxLedger {
			lastLedger = maxLedger
		}
		cmd := exec.Command("stellar-core",
			"--conf", c.getConfFileName(),
			"catchup", fmt.Sprintf("%d/%d", lastLedger, lastLedger - sequence),
			"--replay-in-memory")
		cmd.Dir = c.getTmpDir()
		cmd.Stdout = c.getLogLineWriter()
		cmd.Stderr = cmd.Stdout
		c.cmd = cmd
		e = c.start()
		if e != nil {
			return false, LedgerCloseMeta{}, errors.Wrap(e, "starting stellar-core subprocess")
		}
		// The next ledger should be the first ledger of the checkpoint containing
		// the requested ledger
		c.nextLedger = roundDownToCheckpointStart(sequence)
	}
	if c.metaPipe == nil {
		return false, LedgerCloseMeta{}, errors.New("missing metadata pipe")
	}

	var e error
	for {
		var xlcm xdr.LedgerCloseMeta
		_, e = unmarshalFramed(c.metaPipe, &xlcm)
		if e != nil {
			e = errors.Wrap(e, "unmarshalling framed LedgerCloseMeta")
			break
		}
		seq, e := peekLedgerSequence(&xlcm)
		if e != nil {
			break
		}
		if seq != c.nextLedger {
			// We got something unexpected; close and reset
			e = errors.Errorf("unexpected ledger %d", seq)
			break
		}
		c.nextLedger += 1
		if seq == sequence {
			// Found the requested seq
			var lcm LedgerCloseMeta
			e = c.copyLedgerCloseMeta(&xlcm, &lcm)
			if e != nil {
				break
			}
			return true, lcm, nil
		} else if seq + ledgersPerCheckpoint < sequence {
			// Core somehow started too early, fail.
			e = errors.Errorf("too-early ledger %d", seq)
			break
		} else if seq > sequence {
			// Core somehow overshot target, fail.
			e = errors.Errorf("too-late ledger %d", seq)
			break
		}
	}
	// All paths above that break out of the loop (instead of return)
	// set e to non-nil: there was an error and we should close and
	// reset state before retuning an error to our caller.
	c.Close()
	c.nextLedger = 0
	return false, LedgerCloseMeta{}, e
}

func (c *captiveStellarCore) GetLatestLedgerSequence() (uint32, error) {
	archive, e := historyarchive.Connect(
		c.historyURLs[0],
		historyarchive.ConnectOptions{},
	)
	if e != nil {
		return 0, e
	}
	has, e := archive.GetRootHAS()
	if e != nil {
		return 0, e
	}
	return has.CurrentLedger, nil
}

func (c *captiveStellarCore) Close() error {
	var e0, e1, e2 error
	if c.metaPipe != nil {
		c.metaPipe = nil
	}
	if c.cmd != nil && c.cmd.Process != nil {
		e1 = c.cmd.Process.Kill()
		c.cmd.Process = nil
	}
	e2 = os.RemoveAll(c.getTmpDir())
	if e0 != nil {
		return e0
	}
	if e1 != nil {
		return e1
	}
	if e2 != nil {
		return e2
	}
	return nil
}


func (c *captiveStellarCore) getTmpDir() string {
	return path.Join(os.TempDir(), c.nonce)
}

func (c *captiveStellarCore) getConfFileName() string {
	return path.Join(c.getTmpDir(), "stellar-core.conf")
}

func (c *captiveStellarCore) getConf() string {
	lines := []string{
		"# Generated file -- do not edit",
		"RUN_STANDALONE=true",
		"NODE_IS_VALIDATOR=false",
		"DISABLE_XDR_FSYNC=true",
		"UNSAFE_QUORUM=true",
		fmt.Sprintf(`NETWORK_PASSPHRASE="%s"`, c.networkPassphrase),
		fmt.Sprintf(`BUCKET_DIR_PATH="%s/buckets"`, c.getTmpDir()),
		fmt.Sprintf(`METADATA_OUTPUT_STREAM="%s"`, c.getPipeName()),
	}
	for i, val := range c.historyURLs {
		lines = append(lines, fmt.Sprintf("[HISTORY.h%d]", i))
		lines = append(lines, fmt.Sprintf(`get="curl -sf %s/{0} -o {1}"`, val))
	}
	// Add a fictional quorum -- necessary to convince core to start up;
	// but not used at all for our purposes. Pubkey here is just random.
	lines = append(lines,
		"[QUORUM_SET]",
		"THRESHOLD_PERCENT=100",
		`VALIDATORS=["GCZBOIAY4HLKAJVNJORXZOZRAY2BJDBZHKPBHZCRAIUR5IHC2UHBGCQR"]`)
	return strings.Join(lines, "\n")
}

func (c *captiveStellarCore) getLogLineWriter() io.Writer {
	r, w := io.Pipe()
	br := bufio.NewReader(r)
	go func() {
		for {
			line, e := br.ReadString('\n')
			if e != nil {
				break
			}
			c.lastFewLogLines.addLogLine(line)
		}
	}()
	return w
}

// Makes the temp directory and writes the config file to it; called by the
// platform-specific captiveStellarCore.Start() methods.
func (c *captiveStellarCore) writeConf() error {
	e := os.MkdirAll(c.getTmpDir(), 0755)
	if e != nil {
		return e
	}
	conf := c.getConf()
	return ioutil.WriteFile(c.getConfFileName(), []byte(conf), 0644)
}
