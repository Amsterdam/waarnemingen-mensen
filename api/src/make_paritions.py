#!/usr/bin/python

import psycopg2
from os import environ
from sys import exit
from datetime import datetime, timedelta

PTABLE = "passage_passage"
PARTITIONS_TO_ADD = 6
conn = None
# add 7 partitions in advance if needed (range is a day)
try:
    dbparam = {
        'host': environ['DATABASE_HOST'],
        'dbname': environ['DATABASE_NAME'],
        'user': environ['DATABASE_USER'],
        'password': environ['DATABASE_PASSWORD']
        }
    conn = psycopg2.connect(**dbparam)
except:
    print("connection failed")
    exit(-1)

try:
    # check pg version?, we need 10+ for partitions and 11+ for indexes within the partitions
    cur = conn.cursor()
    cur.execute("""select Substr(setting, 1, strpos(setting, '.')-1)::smallint as version from pg_settings where name = 'server_version';""")
    rows = cur.fetchall()
    if len(rows) == 1 and int(rows[0][0]) < 11:
        print("Need postgres v11 or higher")
        exit(-1)

    start_date = datetime.today().date()
    for i in range(PARTITIONS_TO_ADD):
        partition_date = start_date + timedelta(i)
        partition_str = partition_date.strftime("%Y%m%d")
        create_stm = f"create table if not exists {PTABLE}_{partition_str} partition of {PTABLE} for values from ('{partition_date}') to ('{partition_date + timedelta(1)}');"
        print(f"Creating partition {PTABLE}_{partition_str} ...")
        cur.execute(create_stm)

    conn.commit()
except Exception as e:
    print (e)
finally:
    if conn is not None:
        conn.close()
