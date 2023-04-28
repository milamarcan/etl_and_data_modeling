import os
import glob
import configparser
import psycopg2
import pandas as pd
from sql_queries import *

config = configparser.ConfigParser()
config.read("database.cfg.template")

db_host = config.get("postgres_db1", "host")
db_name = config.get("postgres_db1", "dbname")
db_user = config.get("postgres_db1", "user")
db_password = config.get("postgres_db1", "password")


def process_song_file(cur, filepath):
    """
    Description: Processes all song files from 'data' folder: opens the file, reads it line by line,
    extracts data for songs and artists table and inserts it to the respective tables.

    Arguments:
        cur: the cursor object
        filepath: song data file path

    Returns: None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(
        df[["song_id", "title", "artist_id", "year", "duration"]].values[0]
    )
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(
        df[
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ]
        ].values[0]
    )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: Processes all log files from 'data' folder: opens the file, reads it line by line,
    filter results with 'NextSong' value of the 'Page' field, converts timestamp from
    bigint to datetime data type, inserts data to 'time' table, extract users data,
    inserts it into 'users' table, insert songplay records into 'songplays' table.

    Arguments:
        cur: the cursor object
        filepath: log data file path

    Returns: None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = [
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.weekofyear,
        t.dt.month,
        t.dt.year,
        t.dt.weekday,
    ]
    column_labels = ["start_time", "hour", "day",
                     "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        songplay_data = (
            pd.to_datetime(row.ts, unit="ms"),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: Processes the data: gets all files with the matching extension from the directory,
    prints the result, iterates over files and processes them one by one

    Arguments:
        cur: the cursor object
        conn: connection to the database
        filepath: path to the data file
        func: function that transforms the data and inserts it into the database.

    Returns: None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    """
    Description: connects to the database, gets a cursor, defines where to take the data to process,
    closes the connection

    Arguments: None

    Returns: None
    """
    conn = psycopg2.connect(
        "host=db_host dbname=db_name user=db_user password=db_password"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
