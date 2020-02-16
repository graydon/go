package ledgerbackend

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)


// TODO: test frame decoding
// TODO: test from static base64-encoded data

func TestCaptiveCore(t *testing.T) {
	c := NewCaptive("Public Global Stellar Network ; September 2015",
		[]string{"http://history.stellar.org/prd/core-live/core_live_001"})
	seq, e := c.GetLatestLedgerSequence()
	assert.NoError(t, e)
	assert.Greater(t, seq, uint32(0))
	ok, lcm, e := c.GetLedger(seq-200)
	assert.NoError(t, e)
	assert.Equal(t, true, ok)
	assert.Equal(t, uint32(lcm.LedgerHeader.Header.LedgerSeq), seq-200)
	assert.DirExists(t, c.getTmpDir())
	lines := c.GetRecentLogLines()
	var count int = 0
	for _, line := range lines {
		if strings.Contains(line, "applying ledger") {
			count += 1
		}
	}
	assert.Greater(t, count, 0)
	e = c.Close()
	assert.NoError(t, e)
}
