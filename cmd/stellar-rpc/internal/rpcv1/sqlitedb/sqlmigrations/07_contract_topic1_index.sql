-- +migrate Up
-- Serves contract + topic filters directly. The planner only prefers it once the
-- table has statistics, hence the ANALYZE. Deferred during a backfill bulk-load
-- like the other event indexes, so changing it here affects deferredIndexes in db.go;
-- IF NOT EXISTS covers a DB whose finalize already built it before this migration ran.
CREATE INDEX IF NOT EXISTS idx_id_contract_id_topic1 ON events (contract_id, topic1, id);
ANALYZE events;

-- +migrate Down
DROP INDEX IF EXISTS idx_id_contract_id_topic1;
