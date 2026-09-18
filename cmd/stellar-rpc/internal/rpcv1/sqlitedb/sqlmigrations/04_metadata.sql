-- +migrate Up
DELETE FROM metadata WHERE key = 'LatestLedgerSequence';

-- +migrate Down
INSERT INTO metadata (key, value) VALUES ('LatestLedgerSequence', '0');
