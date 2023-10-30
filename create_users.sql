-- Creates the test users on the various database servers and creates the test database.
-- This would be SCPed to the host and executed as root from the command line.

CREATE USER tim@localhost IDENTIFIED BY 'swordfish123';
GRANT ALL ON *.* TO tim@localhost WITH GRANT OPTION;

CREATE USER tim@'%' IDENTIFIED BY 'swordfish123';
GRANT ALL ON *.* TO tim@'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;
CREATE DATABASE test;
