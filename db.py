import sqlite3
from datetime import datetime

DB_NAME='2021-06'

conn = sqlite3.connect('{}.db', format=DB_NAME)
cur = conn.cursor()

def create_table():
    pass

if __name__ == '__main__':
    create_table()