import configparser


# CONFIG
"""
We'll place the configParser method in a variable and have it read our .cfg file to gather our credentials.
"""
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
"""
We'll delete tables if they already exist so that the creation of the tables doesn't cause an error.
"""
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
"""
We'll create our staging and analytics tables.
"""

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table (artist varchar, auth varchar, firstName varchar, gender varchar, itemInSession int, lastName varchar, length decimal, level varchar, location varchar, method varchar, page varchar, registration decimal, sessionId int, song varchar, status int, ts timestamp, userAgent varchar, userId int);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table (num_songs int, artist_id varchar, artist_latitude varchar, artist_longitude varchar, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration numeric, year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (songplay_id int identity(0,1) PRIMARY KEY, start_time varchar NOT NULL, user_id int NOT NULL, level varchar, song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id int, location varchar, user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (song_id varchar PRIMARY KEY, title varchar, artist_id varchar NOT NULL, year int, duration numeric);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude numeric, longitude numeric);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (start_time timestamp PRIMARY KEY, hour int, day varchar, week int, month varchar, year int, weekday varchar);
""")

# STAGING TABLES
"""
We'll pull the data from the S3 buckets and house them in our staging tables for later use.
"""

staging_events_copy = ("""COPY staging_events_table FROM {} iam_role '{}' FORMAT AS JSON {} TIMEFORMAT AS 'epochmillisecs';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs_table FROM {} iam_role '{}' FORMAT AS JSON 'auto';
""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))

# FINAL TABLES
"""
We'll pull information from our staging tables to apply the proper insertions to our analytics tables.
"""

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
  SELECT DISTINCT 
      TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
      e.userId as user_id, 
      e.level,
      s.song_id,
      s.artist_id,
      e.sessionId as session_id,
      e.location,
      e.userAgent as user_agent
  FROM staging_events e
  JOIN staging_songs s
      ON e.song = s.title AND e.artist = s.artist_name
  WHERE e.page = 'NextSong'
  AND e.song = s.title
""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
  SELECT DISTINCT
      e.userId as user_id,
      e.firstName as first_name,
      e.lastName as last_name,
      e.gender,
      e.level
  FROM staging_events e
  JOIN staging_songs s
      ON e.song = s.title AND e.artist = s.artist_name   
  WHERE e.page = 'NextSong'
  AND e.song = s.title
""")

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration)
  SELECT DISTINCT
      s.song_id,
      s.title,
      s.artist_id,
      s.year,
      s.duration
  FROM staging_events e
  JOIN staging_songs s
      ON e.song = s.title AND e.artist = s.artist_name   
  WHERE e.page = 'NextSong'
  AND e.song = s.title    
""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
  SELECT DISTINCT
      s.artist_id,
      s.artist_name as name,
      s.artist_location as location,
      s.artist_latitude as latitude,
      s.artist_longitude as longitude
  FROM staging_events e
  JOIN staging_songs s
      ON e.song = s.title AND e.artist = s.artist_name   
  WHERE e.page = 'NextSong'
  AND e.song = s.title  
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
  SELECT DISTINCT
      TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
      EXTRACT(HOUR FROM s.ts) as hour,
      EXTRACT(DAY FROM s.ts) as day,
      EXTRACT(WEEK FROM s.ts) as week,
      EXTRACT(MONTH FROM s.ts) as month,
      EXTRACT(YEAR FROM s.ts) as year,
      EXTRACT(WEEKDAY FROM s.ts) as weekday
  FROM staging_events e
  JOIN staging_songs s
      ON e.song = s.title AND e.artist = s.artist_name   
  WHERE e.page = 'NextSong'
  AND e.song = s.title      
""")

# QUERY LISTS
"""
We'll assign some variables to lists of our different queries for quick access when running the etl.py and create_tables.py files.
"""

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
