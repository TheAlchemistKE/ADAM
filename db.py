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
def acceptable(data: str):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True



def find_existing_score(pid: str):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        cur.execute(sql)
        result = cur.fetchone()
        if result != None:
            return result[0]
        else:
            return None
    except Exception as e:
        # print('find_parent', e)
        return False

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

def sql_insert_replace():
    pass

def sql_insert_has_parent():
    pass

def sql_insert_no_parent():
    pass


if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0
    with open('/home/kelyn/Downloads/AdamDataset/RC_2021-06', buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            if score > 10:
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            sql_insert_replace(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
            else:
                if parent_data:
                    sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                else: 
                    sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

