-- Create the database tables we need for the simulation
USE test;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS customers;


CREATE TABLE IF NOT EXISTS customers (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

INSERT INTO customers (name) VALUES ('Andy'), ('Kyle'), ('Tay'), ('Carl'), ('Chris'), ('Matthew'), ('Reece');

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER NOT NULL AUTO_INCREMENT,
    node VARCHAR(10) NOT NULL,
    uuid VARCHAR(36) NOT NULL,
    created DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    customer_id INTEGER NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);
