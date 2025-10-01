-- +migrate Up
DROP INDEX idx_contract_id;
DROP INDEX idx_topic1;

CREATE INDEX idx_id_contract_id ON events (contract_id, id);
CREATE INDEX idx_id_topic1 ON events (topic1, id);

-- +migrate Down
DROP INDEX idx_id_contract_id;
DROP INDEX idx_id_topic1;

CREATE INDEX idx_contract_id ON events (contract_id);
CREATE INDEX idx_topic1 ON events (topic1);
