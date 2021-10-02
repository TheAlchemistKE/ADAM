import sqlite3
import json
from datetime import datetime

timeframe='2021-06'

conn = sqlite3.connect('{}.db'.format(timeframe))
cur = conn.cursor()
sql_transaction = []
start_row = 0
cleanup = 1000000

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

def acceptable(data):
    if len(data.split(' ')) > 1000 or len(data) < 1:
        return False
    elif len(data) > 32000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
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


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        cur.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                cur.execute(s)
            except:
                pass
        conn.commit()
        sql_transaction = []

def sql_insert_replace(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))

def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))


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
            comment_id = row['name']
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
            
            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))

            if row_counter > start_row:
                if row_counter % cleanup == 0:
                    print("Cleanin up!")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    cur.execute(sql)
                    conn.commit()
                    cur.execute("VACUUM")
                    conn.commit()

