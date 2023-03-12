class SqlQueries:
    
    songplay_table_insert = (""" (start_time,userid,level,songid,artistid,sessionid,location,user_agent) 
    
            select TIMESTAMP 'epoch' + staging_events.ts/1000 \
            * INTERVAL '1 second'   AS start_time,
            staging_events.userid                   AS userid,
            staging_events.level                    AS level,
            staging_songs.song_id                   AS songid,
            staging_songs.artist_id                 AS artistid,
            staging_events.sessionid                AS sessionid,
            staging_events.location                 AS location,
            staging_events.useragent                AS user_agent
            FROM staging_events 
            JOIN staging_songs
            ON (staging_events.artist = staging_songs.artist_name)
            WHERE staging_events.page = 'NextSong' and staging_events.userid IS NOT NULL and staging_songs.song_id IS NOT NULL and staging_songs.artist_id IS NOT NULL ;
            """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE staging_songs.artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time) AS hour, extract(day from start_time) AS day, extract(week from start_time) AS week, 
               extract(month from start_time) AS month, extract(year from start_time) AS year, extract(dayofweek from start_time) AS weekday
        FROM songplays
    """)