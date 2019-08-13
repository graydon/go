-- +migrate Up

update history_effects
set details = details || jsonb_build_object('new_seq', (details->> 'new_seq')::text)
where type=43;

-- +migrate Down

update history_effects
set details = details || jsonb_build_object('new_seq', (details->> 'new_seq')::bigint)
where type=43;
