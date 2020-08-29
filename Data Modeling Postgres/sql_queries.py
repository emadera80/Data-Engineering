# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
    create table if not exists songplays ( 
        songplay_id serial primary key, 
        start_time timestamp not null, 
        user_id integer not null,
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id integer, 
        location varchar, 
        user_agent varchar 

    )
""")

user_table_create = ("""
    create table if not exists users (
        user_id integer primary key, 
        first_name varchar not null, 
        last_name varchar not null, 
        gender char(1), 
        level varchar  not null
    )
""")

song_table_create = ("""
    create table if not exists songs ( 
        song_id varchar primary key, 
        title varchar not null, 
        artist_id varchar not null, 
        year integer not null, 
        duration numeric (15,5) not null
    );
""")

artist_table_create = ("""
    create table if not exists artists ( 
        artist_id varchar primary key, 
        name varchar not null, 
        location varchar not null, 
        longitude numeric,
        latitude numeric
        
    )
""")

time_table_create = ("""
    create table if not exists time ( 
        start_time timestamp not null primary key, 
        hour numeric not null, 
        day numeric not null, 
        week numeric not null, 
        month numeric not null,
        year numeric not null,
        weekday numeric not null
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays (  
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id,
        session_id, 
        location, 
        user_agent
    ) values (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    insert into users ( 
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    ) values (%s, %s, %s, %s, %s) 
    on conflict(user_id)
    do update
    set level = EXCLUDED.level
""")

song_table_insert = ("""
    insert into songs (
        song_id,
        title, 
        artist_id, 
        year, 
        duration 
    ) values (%s, %s, %s, %s, %s)
    on conflict(song_id)
    do nothing
""")

artist_table_insert = ("""
    insert into artists (
        artist_id, 
        name, 
        location, 
        longitude, 
        latitude
    ) values (%s, %s, %s, %s, %s)
    on conflict(artist_id)
    do nothing
""")


time_table_insert = ("""
    insert into time ( 
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    ) values (%s, %s, %s, %s, %s, %s, %s)
    on conflict(start_time)
    do nothing
""")

# FIND SONGS

song_select = ("""
SELECT DISTINCT
songs.artist_id
    ,songs.song_id
FROM songs
INNER JOIN artists
    ON artists.artist_id = songs.artist_id
    
WHERE songs.title = %s
AND artists.name = %s
AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]