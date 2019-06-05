-- \COPY project1.artist FROM '/home/cs143/data/artist.csv' DELIMITER ',' CSV;
-- \COPY project1.song FROM '/home/cs143/data/song.csv' DELIMITER ',' CSV;
-- \COPY project1.token FROM '/home/cs143/data/token.csv' DELIMITER ',' CSV;


\COPY project1.artist FROM '/Users/mnallamalli97/Desktop/143/shared_folder/artist.csv' DELIMITER ',' CSV;
\COPY project1.song FROM '/Users/mnallamalli97/Desktop/143/shared_folder/song.csv' DELIMITER ',' CSV;
\COPY project1.token FROM '/Users/mnallamalli97/Desktop/143/shared_folder/token.csv' DELIMITER ',' CSV;

--create table for TF_ID
CREATE TABLE Project1.tfidf AS

select sid, t2.token, (t2.count* log((57650/t2.no_of_songs))) as tfidf
from
(
	select sid, L.token, count, R.no_of_songs
	from project1.token L
	inner join 
	(
		select token, count(token) as no_of_songs from project1.token group by token
	) R
	on L.token = R.token
) t2;

