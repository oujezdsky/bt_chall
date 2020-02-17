import os

DEBUG = False
ASYNC_CONSUMER = False
ENCODING = 'utf-8'
TABLE_NAME = 'item'
CHANNEL_NAME = 'item_change'
FD_TIMEOUT = 5
TABLE_KEYS = ('id', 'status', 'updated_at', 'logged_at')
DSN = 'postgresql://user:pw@localhost:5432/db_name'
DSN_AIOPG = 'dbname=x user=x password=x host=x'
