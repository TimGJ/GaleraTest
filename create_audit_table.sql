-- We need a separate audit table initially independent of the cluster which records all the transactions.
-- This should tally with the cluster.

USE test;
DROP TABLE IF EXISTS audit;

CREATE TABLE IF NOT EXISTS audit (
    id INTEGER NOT NULL AUTO_INCREMENT,
    node VARCHAR(10) NOT NULL,
    uuid VARCHAR(36) NOT NULL,
    created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    customer_id INTEGER NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (id)
);