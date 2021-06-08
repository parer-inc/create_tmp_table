"""This service allows to create temporary tables in db"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor

r = get_redis()

def create_tmp_table(name):
    """Create new tmp table"""
    cursor, db = get_cursor()
    if not cursor or not db:
        # log that failed getting cursor
        return False
    if "tmp" not in name:
        # log that name was wrong
        return False
    try:
        cursor.execute(f"""CREATE TABLE `{name}`(
                        `id` int AUTO_INCREMENT,
                        `data` varchar(50),
                        primary key (id)
                        )
                       """)
    except MySQLdb.Error as error:
        print(error)
        # Log
        return False
    db.commit()
    cursor.close()
    return True


if __name__ == '__main__':
    q = Queue('create_tmp_table', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r, name='create_tmp_table')
        worker.work()
