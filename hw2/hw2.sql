DROP SCHEMA IF EXISTS hw2 CASCADE;
CREATE SCHEMA hw2;

-- Data for part 1.

DROP TABLE IF EXISTS hw2.caltrans;
CREATE TABLE hw2.caltrans (
  reported timestamp DEFAULT CURRENT_TIMESTAMP,
  highway varchar(6) NOT NULL,
  area varchar(255) NOT NULL,
  condition text NOT NULL,
  hash varchar(32) NOT NULL,
  PRIMARY KEY (reported, highway, area, hash)
);



-- Data for part 3.
-- For simplicity, this does not match HW 1 intentionally.
DROP TABLE IF EXISTS hw2.user;
DROP TABLE IF EXISTS hw2.trip_start;
DROP TABLE IF EXISTS hw2.trip_end;
CREATE TABLE hw2.user (
	user_id smallint PRIMARY KEY,
	ccnumber varchar(19),
	expiration timestamp,
	email varchar(255)
);
CREATE TABLE hw2.trip_start (
	trip_id integer,
	user_id integer,
	scooter_id smallint,
	time timestamp,
	lat double precision,
	lon double precision,
	PRIMARY KEY(trip_id, user_id)
);

/* point is preferably over lat and lon as double precision
but it's not trivial trying to load data into the schema
using the \copy command without PostGIS.*/

CREATE TABLE hw2.trip_end (
	trip_id integer,
	user_id integer,
	scooter_id smallint,
	time timestamp,
	lat double precision,
	lon double precision,
	PRIMARY KEY(trip_id, user_id)
);

\copy hw2.caltrans from 'caltrans.csv' WITH DELIMITER ',' QUOTE '"' CSV;
\copy hw2.trip_start from 'trip_start.csv' WITH DELIMITER ',' QUOTE '"' CSV;
\copy hw2.trip_end from 'trip_end.csv' WITH DELIMITER ',' QUOTE '"' CSV;
\copy hw2.user from 'user.csv' WITH DELIMITER ',' QUOTE '"' CSV;
