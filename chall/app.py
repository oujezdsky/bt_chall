from chall.constants import DEBUG, ASYNC_CONSUMER, TABLE_NAME
from chall.routine import ChanListener, AioConsumer
import logging
import sys
import asyncio

logging.basicConfig(
    filename="error.log",
    filemode="a",
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    level=logging.ERROR,
)

logger = logging.getLogger(__name__)


class App(object):
    def run(self):
        chan_listener = ChanListener()

        try:
            chan_listener.init_db(recreate=0)
            if ASYNC_CONSUMER:
                loop = asyncio.get_event_loop()
                aio = AioConsumer()
                loop.run_until_complete(aio.main())
            else:
                chan_listener.consume_select()
        except Exception as e:
            if chan_listener.conn:
                chan_listener.conn.close()
            logger.error(e)
            return
