-- +migrate Up

-- indexing table to find events in ledgers by contract_id
CREATE TABLE events
(
    id                TEXT PRIMARY KEY,
    contract_id       BLOB(32),
    event_type        INTEGER NOT NULL,
    event_data        BLOB    NOT NULL,
    ledger_close_time INTEGER NOT NULL,
    transaction_hash  BLOB(32),
    topic1            BLOB,
    topic2            BLOB,
    topic3            BLOB,
    topic4            BLOB
);

CREATE INDEX idx_contract_id ON events (contract_id);
CREATE INDEX idx_topic1 ON events (topic1);


-- +migrate Down
drop table events cascade;
