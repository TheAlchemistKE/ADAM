import sqlite3
import json
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

def format_data(data: str):
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data

def find_parent(pid: str):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        cur.execute(sql)
        result = cur.fetchone()
        if result != None:
            return result[0]
        else:
            return None
    except Exception as e:
        # print('find_parent', e)
        return False
    



if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0
    with open('/home/kelyn/Downloads/AdamDataset/RC_2021-06', buffer=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']

