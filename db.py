import sqlite3
from datetime import datetime

timeframe='2021-06'

conn = sqlite3.connect('{}.db'.format(timeframe))
cur = conn.cursor()

def create_table():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS parent_reply (
        parent_id TEXT PRIMARY KEY,
        comment_id TEXT UNIQUE,
        parent TEXT,
        comment TEXT,
        subreddit TEXT,
        unix INT,
        score INT
    )
    """)

if __name__ == '__main__':
    create_table()