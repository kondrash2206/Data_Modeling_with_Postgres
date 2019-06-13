import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    '''
    Function that processes a single "song file"
    Input: "cur" db cursor, "filepath" (str) filepath to the song file
    '''
    
    # open song file
    df = pd.read_json(filepath, typ = 'series')
    
    # define songs columns
    columns_songs = ['song_id','title','artist_id','year','duration']
    
    # modify and filter songs dataframe
    song_df = df.to_frame('').transpose()[columns_songs]
    
    # insert song record
    song_data = list(song_df.values[0])
    cur.execute(song_table_insert, song_data)
    
    # define artists columns
    columns_artists = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    
    # modify and filter artists dataframe
    artists_df = df.to_frame('').transpose()[columns_artists]

    # insert artist record
    artist_data = list(artists_df.values[0]) 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Function that processes a single "log file"
    Input: "cur" db cursor, "filepath" (str) filepath to the log file
    '''
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit = 'ms')
    
    # insert time data records
    time_df = pd.DataFrame()
    time_df['ts'] = df['ts']
    time_df['hour'] = t.dt.hour
    time_df['day'] = t.dt.day
    time_df['week'] = t.dt.week
    time_df['month'] = t.dt.month
    time_df['year'] = t.dt.year
    time_df['week_day'] = t.dt.weekday
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    columns_users = ['userId','firstName','lastName','gender','level']
    user_df = df.loc[df['userId'].astype(str) != '' ,columns_users]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row['ts'],row['userId'],row['level'],songid,artistid, row['sessionId'], row['location'], row['userAgent'] )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Function that processes a single "song file"
    Input: "cur" db cursor, "conn" db connection, "filepath" (str) filepath to the data folder, 
            "func" a function name: either "process_song_file" or "process_log_file"
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()