DROP SCHEMA IF EXISTS hw3 CASCADE;
CREATE SCHEMA hw3;

DROP TABLE IF EXISTS hw3.keyvalues CASCADE;
CREATE TABLE hw3.keyvalues (
    company varchar(20),
    value   varchar(50),
    PRIMARY KEY(company, value)
);

\copy hw3.keyvalues from 'keyvalues.csv' WITH DELIMITER ',' QUOTE '"' CSV;
