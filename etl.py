import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """
    Process a single song file and insert records into the songs and artists tables.

    Parameters:
        cur: Cursor of the database.
        filepath: Filepath to the song JSON file.
    """
    df = pd.read_json(filepath, lines=True)

    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    Process a single log file and insert records into time, users, and songplays tables.

    Parameters:
        cur: Cursor of the database.
        filepath: Filepath to the log JSON file.
    """
    df = pd.read_json(filepath, lines=True)

    df = df[df['page'] == 'NextSong']

    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        song_title = row.song
        artist_name = row.artist
        song_duration = round(row.length, 2) if row.length else None

        cur.execute(song_select, (song_title, artist_name, song_duration - 2, song_duration + 2))
        results = cur.fetchone()
        song_id, artist_id = results if results else (None, None)

        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    Process all files in a given directory using the provided processing function.

    Parameters:
        cur: Cursor of the database.
        conn: Connection to the database.
        filepath: Root directory of files.
        func: Processing function to be used on each file.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')

def main():
    """
    Establish connection to the database and initiate ETL pipeline.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
