# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DDROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (songplay_id serial PRIMARY KEY NOT NULL, start_time text REFERENCES time(start_time) NOT NULL, 
user_id text REFERENCES users(user_id) NOT NULL, song_id text REFERENCES songs(song_id), artist_id text REFERENCES artists(artist_id),
session_id text NOT NULL, location text, user_agent text NOT NULL)
""")

user_table_create = ("""
CREATE TABLE users (user_id text PRIMARY KEY NOT NULL, first_name text NOT NULL, last_name text NOT NULL, gender text NOT NULL, level text NOT NULL)
""")

song_table_create = ("""
CREATE TABLE songs (song_id text PRIMARY KEY NOT NULL, title text NOT NULL,artist_id text REFERENCES artists(artist_id)NOT NULL, year int, duration float NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE artists (artist_id text PRIMARY KEY NOT NULL, artist_name text NOT NULL, artist_location text, artist_latitude text, artist_longitude text)
""")

time_table_create = ("""
CREATE TABLE time (start_time text PRIMARY KEY NOT NULL, hour text NOT NULL, day text NOT NULL, week text NOT NULL, month text NOT NULL, year int NOT NULL, weekday text NOT NULL)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE
set level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs 
(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
select song_id, songs.artist_id
from songs
join artists on artists.artist_id = songs.artist_id
where title = %s 
and artist_name = %s
and duration = %s
""")

# QUERY LISTS

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [artist_table_create, song_table_create, user_table_create,  time_table_create, songplay_table_create]
