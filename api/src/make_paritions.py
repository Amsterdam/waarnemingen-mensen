#!/usr/bin/python

import psycopg2
from os import environ
from sys import exit
from datetime import datetime, timedelta
import logging

PTABLE = "passage_passage"
PARTITIONS_TO_ADD = 6
conn = None

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# add 7 partitions in advance if needed (range is a day)
try:
    dbparam = {
        'host': environ['DATABASE_HOST'],
        'dbname': environ['DATABASE_NAME'],
        'user': environ['DATABASE_USER'],
        'password': environ['DATABASE_PASSWORD']
        }
    conn = psycopg2.connect(**dbparam)
except Exception as e:
    print("connection failed")
    print(e)
    log.error(e)
    log.error("Database connection Failed!")
    log.error(dbparam)
    exit(-1)


SQL_VERSION = """
    SELECT Substr(setting, 1, strpos(setting, '.')-1)::smallint as version
    FROM pg_settings
    WHERE name = 'server_version';
"""

try:
    # check pg version?, we need 10+ for partitions and 11+ for
    # indexes within the partitions
    cur = conn.cursor()
    cur.execute(SQL_VERSION)
    rows = cur.fetchall()

    if len(rows) == 1 and int(rows[0][0]) < 11:
        print("Need postgres v11 or higher")
        exit(-1)

    start_date = datetime.today().date()
    for i in range(PARTITIONS_TO_ADD):
        partition_date = start_date + timedelta(i)
        partition_str = partition_date.strftime("%Y%m%d")

        SQL_PARTITION_DIE_SHIT = f"""
        CREATE table IF NOT EXISTS {PTABLE}_{partition_str}
        PARTITION OF {PTABLE}
        FOR VALUES
        FROM ('{partition_date}') TO ('{partition_date + timedelta(1)}');
        """
        create_stm = SQL_PARTITION_DIE_SHIT

        print(create_stm)
        print(f"Creating partition {PTABLE}_{partition_str} ...")
        cur.execute(create_stm)

    conn.commit()
except Exception as e:
    print(e)
finally:
    if conn is not None:
        conn.close()
