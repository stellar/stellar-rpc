-- +migrate Up
DROP TABLE ledger_entries;

-- +migrate Down
CREATE TABLE ledger_entries (
    key BLOB NOT NULL PRIMARY KEY,
    entry BLOB NOT NULL
);