import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('config.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS sonplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF  EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events( 
        ARTIST           VARCHAR, 
        AUTH             VARCHAR, 
        FIRSTNAME        VARCHAR,
        GENDER           VARCHAR, 
        ITEMINSESSION    INTEGER, 
        LASTNAME         VARCHAR, 
        LENGTH           FLOAT, 
        LEVEL            VARCHAR, 
        LOCATION         VARCHAR, 
        METHOD           VARCHAR,
        PAGE             VARCHAR, 
        REGISTRATION     FLOAT, 
        SESSIONID        INTEGER, 
        SONG             VARCHAR, 
        STATUS           INTEGER,
        TS               TIMESTAMP,
        USERAGENT        VARCHAR, 
        USERID           INTEGER
        
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        NUM_SONGS           INTEGER, 
        ARTIST_ID           VARCHAR, 
        ARTIST_LATITUDE     FLOAT, 
        ARTIST_LONGITUDE    FLOAT, 
        ARTIST_LOCATION     VARCHAR, 
        ARTIST_NAME         VARCHAR, 
        SONG_ID             VARCHAR, 
        TITLE               VARCHAR, 
        DURATION            FLOAT, 
        YEAR                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE sonplays(
        SONGPLAY_ID     INTEGER     IDENTITY(0,1) PRIMARY KEY, 
        START_TIME      TIMESTAMP   NOT NULL SORTKEY DISTKEY, 
        USER_ID         INTEGER     NOT NULL, 
        LEVEL           VARCHAR, 
        SONG_ID         VARCHAR     NOT NULL, 
        ARTIST_ID       VARCHAR     NOT NULL, 
        SESSION_ID      INTEGER, 
        LOCATION        VARCHAR, 
        USER_AGENT      VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE users( 
        USER_ID     INTEGER   NOT NULL SORTKEY PRIMARY KEY, 
        FIRST_NAME  VARCHAR   NOT NULL, 
        LAST_NAME   VARCHAR   NOT NULL, 
        GENDER      VARCHAR   NOT NULL, 
        LEVEL       VARCHAR   NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE songs ( 
        SONG_ID      VARCHAR       NOT NULL SORTKEY PRIMARY KEY, 
        TITLE        VARCHAR       NOT NULL, 
        ARTIST_ID    VARCHAR       NOT NULL, 
        YEAR         INTEGER       NOT NULL, 
        DURATION     FLOAT 
    )
""")

artist_table_create = ("""
    CREATE TABLE artists( 
        ARTIST_ID     VARCHAR   NOT NULL SORTKEY PRIMARY KEY, 
        NAME          VARCHAR   NOT NULL, 
        LOCATION      VARCHAR, 
        LATITUDE      FLOAT, 
        LONGITUDE     FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE time( 
        START_TIME     TIMESTAMP    NOT NULL DISTKEY SORTKEY PRIMARY KEY, 
        HOUR           INTEGER      NOT NULL, 
        DAY            INTEGER      NOT NULL, 
        WEEK           INTEGER      NOT NULL, 
        MONTH          INTEGER      NOT NULL, 
        YEAR           INTEGER      NOT NULL, 
        WEEKDAY        VARCHAR(20)  NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO sonplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT distinct (e.ts)
        , e.userid
        , e.level
        , s.song_id
        , s.artist_id
        , e.sessionid
        , e.location
        , e.userAgent
    FROM staging_events e 
    JOIN staging_songs s on (e.song = s.title and e.artist = s.artist_name)
    AND e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT (userId)
        , firstname
        , lastname
        , gender 
        , level 
    FROM staging_events 
    WHERE userId IS NOT NULL 
    and page = 'NextSong'; 
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT (song_id)
        , title
        , artist_id
        , year
        , duration
    FROM staging_songs 
    WHERE song_id IS NOT NULL; 
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT (artist_id)
        , artist_name
        , artist_location
        , artist_latitude
        , artist_longitude
    FROM staging_songs 
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(start_time)
        , EXTRACT(hour FROM start_time)
        , EXTRACT(day FROM start_time)
        , EXTRACT(week FROM start_time)
        , EXTRACT(month FROM start_time)
        , EXTRACT(year FROM start_time)
        , EXTRACT(dayofweek FROM start_time)
    FROM sonplays;
""")

test_staging_events=("""
    SELECT COUNT(*) FROM staging_events
""")

test_staging_songs=("""
    SELECT COUNT(*) FROM staging_songs
""")

test_songplays=("""
    SELECT COUNT(*) FROM sonplays
""")

test_users=("""
    SELECT COUNT(*) FROM users
""")

test_songs=("""
    SELECT COUNT(*) FROM songs
""")

test_artists=("""
    SELECT COUNT(*) FROM artists
""")

test_time=("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
test_queries = [test_staging_events,test_staging_songs,test_songplays,test_users,test_songs,test_artists,test_time]