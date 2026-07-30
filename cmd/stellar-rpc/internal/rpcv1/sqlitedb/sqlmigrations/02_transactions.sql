-- +migrate Up

-- indexing table to find transactions in ledgers by hash
CREATE TABLE transactions (
    hash BLOB PRIMARY KEY, -- 32-byte binary
    ledger_sequence INTEGER NOT NULL,
    application_order INTEGER NOT NULL
);

CREATE INDEX index_ledger_sequence ON transactions(ledger_sequence);

-- +migrate Down
drop table transactions cascade;
