import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Process a single song json by reading the file into a dataframe and insering records into 
    the artists and songs tables"""
    # open song file
    df = pd.read_json(filepath,typ='dataframe')

    
    # insert artist record
    artist_df = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data = artist_df.values.astype(str).tolist()
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    songs_df = df[['song_id','title','artist_id','year','duration']]
    song_data = songs_df.values.astype(str).tolist()    
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """Process a single log file, reading in a json and inserting data into the users, time, and songplays
    tables"""
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']
    
    
    # convert timestamp column to datetime
    t = pd.DataFrame(df['ts'])
    t['ts'] = pd.to_datetime(t['ts'],unit='ms')
    t['hour'] = t['ts'].dt.hour
    t['day'] = t['ts'].dt.day
    t['week'] = t['ts'].dt.week
    t['month'] = t['ts'].dt.month
    t['year'] = t['ts'].dt.year
    t['weekday'] = t['ts'].dt.weekday_name
    # convert the ts column in the log df as well
    df['ts'] = pd.to_datetime(df['ts'],unit='ms').astype(str)
    # insert time data records
    time_data = t.to_dict() 
    column_labels = ['start_time','hour','day','week','month','year','weekday'] 
    time_df = pd.DataFrame.from_dict(time_data)
    time_df.columns = column_labels
    time_df['start_time'] = time_df['start_time'].astype(str)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(df[['userId','firstName','lastName','gender','level']])
    user_df.columns = ['user_name','first_name','last_name','gender','level']
    
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
        songplay_data = [row['ts'],row['userId'],songid, artistid,row['sessionId'], row['location'],row['userAgent']]
 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Handler function to find all files given a base path and call a unction with them, as well as print out 
    status information on how many files were processed"""
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
    """Main method of the etl script that will call the process data function for both the song and log file paths"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()