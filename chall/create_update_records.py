from chall.constants import DEBUG, TABLE_NAME
from chall.routine import ChanListener
from sqlalchemy.dialects.postgresql import insert


class Emitter(object):
    def __init__(self):
        self.chan_listener = ChanListener()
        try:
            self.chan_listener.init_db(recreate=1)
        except:
            print('early exit')
            return

    def run(self, affected_rows=10):
        PLD_CREATE = tuple(dict(id=x, status="a" * x) for x in range(affected_rows))
        PLD_UPDATE = tuple(dict(id=x, status="b" * x) for x in range(affected_rows))

        def create_rows():
            self.chan_listener.engine.execute(
                self.chan_listener.meta.tables[TABLE_NAME].insert(), PLD_CREATE
            )

        def update_rows():
            for pld in PLD_UPDATE:
                self.chan_listener.engine.execute(
                    self.chan_listener.meta.tables[TABLE_NAME]
                    .update()
                    .where(
                        self.chan_listener.meta.tables[TABLE_NAME].c.id == pld.get("id")
                    )
                    .values(dict(status=pld.get("status")))
                )

        create_rows()
        update_rows()

