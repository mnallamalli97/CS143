--drop so no error
DROP SCHEMA Project1 CASCADE;

--create the scheme for the project
CREATE SCHEMA Project1;

--create table for Project1.artist_table
CREATE TABLE Project1.artist(
	aId INTEGER PRIMARY KEY, 
	aName VARCHAR
	);

--create table for Project1.song_table
CREATE TABLE Project1.song(
	sId INTEGER PRIMARY KEY, 
	aId INTEGER REFERENCES Project1.artist(aId),
	sName VARCHAR,
	url VARCHAR 
	);

--create table for Project1.token_table
CREATE TABLE Project1.token(
	sId INTEGER REFERENCES Project1.song(sId), 
	token VARCHAR,
	count SMALLINT
	);