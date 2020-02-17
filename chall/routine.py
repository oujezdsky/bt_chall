from sqlalchemy import create_engine, MetaData
from chall.constants import (
    DSN,
    DEBUG,
    ENCODING,
    TABLE_NAME,
    FD_TIMEOUT,
    TABLE_KEYS,
    CHANNEL_NAME,
    DSN_AIOPG,
)
import select
import psycopg2
from sqlalchemy import Table, Column, select, text as sql_text
from sqlalchemy.dialects.postgresql import BIGINT, VARCHAR, TIMESTAMP
from sqlalchemy.exc import OperationalError, UnboundExecutionError

# utils
import datetime
import json

# select
import select
import psycopg2
import threading
from multiprocessing import Queue

# asyncio
import asyncio
import aiopg
from aiopg.sa import create_engine as aiosa__create_engine


class ChanListener(object):
    def __init__(self):
        self.engine = None
        self.conn = None
        self.meta = None

    def init_db(self, recreate=0):
        # complete table setup
        self.create_engine()
        self.create_metadata()
        self.reflect_remote_db()
        self.table_exist = self.table_exists()
        if not self.table_exist and recreate:
            self.create_table_model()
            self.create_table()

        elif recreate:
            self.drop_table()
            self.create_table()

    def create_engine(self):
        # bind db engine
        try:
            self.engine = create_engine(
                DSN, encoding=ENCODING, echo=DEBUG, pool_size=20, max_overflow=0)
        except OperationalError as e:
            raise e

    def create_metadata(self):
        # create metadata
        try:
            self.meta = MetaData(self.engine)
        except UnboundExecutionError as e:
            raise e

    def reflect_remote_db(self):
        # fetch db server
        self.meta.reflect(bind=self.engine)

    def create_table_model(self):
        # setup table model
        self.model = Table(
            TABLE_NAME,
            self.meta,
            Column("id", primary_key=True),
            Column("status", VARCHAR(255), nullable=False),
            Column("updated_at", TIMESTAMP(120), default=None),
            Column("logged_at", TIMESTAMP(120)),
        )

    def table_exists(self):
        return self.meta.tables.get(TABLE_NAME, 0) != 0

    def create_table(self):
        # create table, procedure and function
        self.meta.tables[TABLE_NAME].create(self.engine)
        self.create_procedure()
        self.create_trigger()
        self.table_exist = 1

    def drop_table(self):
        # drop table and pg_function
        self.meta.tables[TABLE_NAME].drop(self.engine)
        self.execute_sql("DROP FUNCTION IF EXISTS notify_trigger();")

    def create_procedure(self):
        # create procedure which send notification
        self.execute_sql(
            """\
                CREATE FUNCTION notify_trigger() RETURNS trigger AS $trigger$
                DECLARE
                rec RECORD;
                BEGIN
                rec := NEW;
                PERFORM pg_notify('item_change','');
                RETURN rec;
                END;
                $trigger$ LANGUAGE plpgsql;
                """
        )

    def create_trigger(self):
        # crete col update trigger
        self.execute_sql(
            """\
                CREATE TRIGGER item_status_update 
                AFTER UPDATE OF status ON item
                FOR EACH ROW 
                EXECUTE PROCEDURE notify_trigger(
                    'id',
                    'status',
                    'updated_at',
                    'logged_at'
                );
                """
        )

    def execute_sql(self, pld: str):
        # execute raw sql command
        try:
            self.engine.execute(sql_text(pld))
        except Exception as e:
            raise e

    def consume_select(self):
        def connect():
            self.conn = self.engine.connect()

        def timeout():
            pass

        def got_notify(notify, q):
            select_query = self.meta.tables[TABLE_NAME].select()
            res = self.engine.execute(select_query)
            for row in res:
                q.put(dict(row))

        def notify_processor(q):
            def _write(pld):
                f = open("output.log", "a")
                f.write(f"{datetime.datetime.now().isoformat()} {pld}\n")
                f.close()

            def json_serial(obj):
                """JSON serializer for objects not serialTABLE_KEYSizable by default json code"""

                if isinstance(obj, (datetime.datetime, datetime.date)):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")

            while True:
                item = q.get()
                _write(json.dumps(item, default=json_serial))
                query = sql_text(
                    f"UPDATE {TABLE_NAME} SET logged_at=:curr_date WHERE id=:the_id"
                )
                query = query.bindparams(
                    the_id=item.get("id"), curr_date=datetime.datetime.now()
                )
                self.engine.execute(query)

        connect()
        q = Queue()
        conn = self.conn.connection.connection
        conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        rlist = [conn]
        curs = self.conn.connection.cursor()
        curs.execute(f"LISTEN {CHANNEL_NAME};")
        npt = threading.Thread(target=notify_processor, args=(q,))
        npt.start()

        while True:
            if select.select(rlist, [], [], FD_TIMEOUT) == ([], [], []):
                timeout()
            else:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop()
                    t = threading.Thread(target=got_notify, args=(notify, q))
                    t.start()


class AioConsumer(object):

    async def notify_processor(self, q, engine):
        async def _write(pld):
            f = open("output.log", "a")
            f.write(f"{datetime.datetime.now().isoformat()} {pld}\n")
            f.close()

        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code"""

            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        while 1:
            item = await q.get()
            await _write(json.dumps(item, default=json_serial))
            query = sql_text(
                f"UPDATE {TABLE_NAME} SET logged_at=:curr_date WHERE id=:the_id"
            )
            query = query.bindparams(
                the_id=item.get("id"), curr_date=datetime.datetime.now()
            )
            async with engine.acquire() as conn:

                await conn.execute(query)

            q.task_done()

    async def runner(self, q):
        async with aiosa__create_engine(DSN) as engine:
            while 1:
                tasks = []
                for i in range(1):
                    task = asyncio.create_task(
                        self.notify_processor(q, engine))
                    tasks.append(task)

                await q.join()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def listen(self, conn, q):

        async with conn.cursor() as cur:
            await cur.execute(f"LISTEN {CHANNEL_NAME}")
            while True:
                _ = await conn.notifies.get()
                await cur.execute("SELECT * FROM item")
                res = await cur.fetchall()
                for r in res:
                    q.put_nowait(dict(zip(TABLE_KEYS, r)))

    async def main(self):
        q = asyncio.Queue()
        async with aiopg.create_pool(DSN_AIOPG) as pool:
            async with pool.acquire() as conn1:
                listener = self.listen(conn1, q)
                runner = self.runner(q)
                await asyncio.gather(listener, runner)
