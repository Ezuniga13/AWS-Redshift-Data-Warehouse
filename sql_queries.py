import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
        (artist             VARCHAR,
         auth               VARCHAR,
         firstName          VARCHAR,
         gender             VARCHAR,
         itemInSession      INTEGER,
         lastName           VARCHAR,
         length             DOUBLE PRECISION,
         level              VARCHAR,
         location           VARCHAR,
         method             VARCHAR,
         page               VARCHAR,
         registration       DOUBLE PRECISION,
         sessionId          INTEGER,
         song               VARCHAR,
         status             INTEGER,
         ts                 BIGINT,
         userAgent          VARCHAR,
         userId             INTEGER
         )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
        (num_songs            INTEGER,
         artist_id            VARCHAR,
         artist_latitude      DOUBLE PRECISION,
         artist_longitude     DOUBLE PRECISION,
         artist_location      VARCHAR,
         artist_name          VARCHAR,
         song_id              VARCHAR,
         title                VARCHAR,
         duration             DOUBLE PRECISION,
         year                 INTEGER
        )
    
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
        (songplay_id    INTEGER IDENTITY (0,1)  PRIMARY KEY, 
        start_time      TIMESTAMP               NOT NULL, 
        user_id         INTEGER                 NOT NULL, 
        level           VARCHAR,
        song_id         VARCHAR, 
        artist_id       VARCHAR,
        session_id      INTEGER, 
        location        VARCHAR, 
        user_agent      VARCHAR)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
        (user_id    INTEGER      PRIMARY KEY, 
        first_name  VARCHAR      NOT NULL, 
        last_name   VARCHAR, 
         gender     VARCHAR, 
         level      VARCHAR)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs 
        (song_id   VARCHAR            PRIMARY KEY, 
        title      VARCHAR            NOT NULL, 
        artist_id  VARCHAR, 
        year       INTEGER, 
        duration   DOUBLE PRECISION   NOT NULL)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
        (artist_id  VARCHAR            PRIMARY KEY,
        name        VARCHAR            NOT NULL, 
        location    VARCHAR, 
        latitude    DOUBLE PRECISION, 
        longitude   DOUBLE PRECISION)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time 
        (start_time TIMESTAMP PRIMARY KEY, 
        hour        INTEGER, 
        day         INTEGER, 
        week        INTEGER, 
        month       INTEGER, 
        year        INTEGER, 
        weekday     VARCHAR )
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events 
    from {} 
    iam_role {}
    region 'us-west-2'
    COMPUPDATE OFF
    json {};
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'],
            )


staging_songs_copy = (""" 
    copy staging_songs 
    from {} 
    iam_role {}
    region 'us-west-2'
    COMPUPDATE OFF
    json 'auto';
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, 
                           location,   user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,                                                                   se.userId, se.level, so.song_id, 
                                      so.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se
    JOIN staging_songs so     ON  (se.artist = so.artist_name AND se.song = so.title)
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events 
    WHERE userId is not null;
    
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location,
                         artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, EXTRACT(hour FROM start_time), 
                    EXTRACT(day FROM start_time), EXTRACT(week FROM start_time),
                    EXTRACT(month FROM start_time), EXTRACT(year FROM start_time),
                    EXTRACT(dayofweek FROM start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,          
                        artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop,staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, 
                      song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]


insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
