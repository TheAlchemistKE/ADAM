import sqlite3
import pandas as pd

timeframes = ['2021-06']

for timeframe in timeframes:
    conn = sqlite3.connect('{}.db'.format(timeframe))
    cur = conn.cursor()
    limit = 5000
    last_unix = 0
    
