from chall.constants import DEBUG
from chall.routine import ChanListener
import logging
import sys

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
            chan_listener.init_db()
            chan_listener.consume_select()
        except Exception as e:
            if chan_listener.conn:
                chan_listener.conn.close()
            logger.error(e)
            return

