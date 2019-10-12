from data_recorder.bitfinex_connector.bitfinex_client import BitfinexClient
from data_recorder.coinbase_connector.coinbase_client import CoinbaseClient
from configurations.configs import SNAPSHOT_RATE, BASKET
from threading import Thread, Timer
from datetime import datetime as dt
from multiprocessing import Process
import time
import asyncio
import logging


logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger('recorder')


class Recorder(object):

    def __init__(self, symbols):
        """
        Constructor of Recorder.

        :param symbols: basket of securities to record...
                        Example: symbols = [('BTC-USD, 'tBTCUSD')]
        """
        self.symbols = symbols
        self.timer_frequency = SNAPSHOT_RATE
        self.clients = dict()
        self.current_time = dt.now()

    def run(self):
        """
        Main loop to instantiate limit order books for
            (1) Coinbase Pro, and
            (2) Bitfinex.
        Connections made to each exchange are made asynchronously thanks to asyncio.
        :return: void
        """
        coinbase, bitfinex = self.symbols

        self.clients[coinbase] = CoinbaseClient(coinbase)
        self.clients[bitfinex] = BitfinexClient(bitfinex)

        threads = [Thread(target=lambda r: r.run(),
                            args=(self.clients[sym],),
                            name=sym, daemon=True) for sym in [coinbase, bitfinex]]
        [thread.start() for thread in threads]

        Timer(5.0, self.timer_worker,
              args=(self.clients[coinbase], self.clients[bitfinex],)).start()

        tasks = asyncio.gather(*[self.clients[sym].subscribe()
                                 for sym in self.clients.keys()])
        loop = asyncio.get_event_loop()
        print('Recorder: Gathered %i tasks' % len(self.clients.keys()))

        try:
            loop.run_until_complete(tasks)
            loop.close()
            [thread.join() for thread in threads]
            logger.info('Recorder: loop closed for %s and %s.' %
                        (coinbase, bitfinex))

        except KeyboardInterrupt as e:
            logger.info("Recorder: Caught keyboard interrupt. \n%s" % e)
            tasks.cancel()
            loop.close()
            [thread.join() for thread in threads]

        finally:
            loop.close()
            logger.info('Recorder: Finally done for %s and %s.' %
                        (coinbase, bitfinex))

    def timer_worker(self, coinbaseClient: CoinbaseClient,
                     bitfinexClient: BitfinexClient):
        """
        Thread worker to be invoked every N seconds
        (e.g., configs.SNAPSHOT_RATE)
        :param coinbaseClient: CoinbaseClient
        :param bitfinexClient: BitfinexClient
        :return: void
        """
        Timer(self.timer_frequency, self.timer_worker,
              args=(coinbaseClient, bitfinexClient,)).start()
        self.current_time = dt.now()

        if coinbaseClient.book.done_warming_up() & \
                bitfinexClient.book.done_warming_up():
            """
            This is the place to insert a trading model. 
            You'll have to create your own.
            
            Example:
                orderbook_data = tuple(coinbaseClient.book, bitfinexClient.book)
                model = agent.dqn.Agent()
                fix_api = SomeFixAPI() 
                action = model(orderbook_data)
                if action is buy:
                    buy_order = create_order(pair, price, etc.)
                    fix_api.send_order(buy_order)
            
            """
            logger.info('%s >> %s' % (coinbaseClient.sym, coinbaseClient.book))
        else:
            if coinbaseClient.book.done_warming_up():
                logger.info('Coinbase - %s is warming up' % coinbaseClient.sym)
            if bitfinexClient.book.done_warming_up():
                logger.info('Bitfinex - %s is warming up' % bitfinexClient.sym)


def main():
    logger.info('Starting recorder with basket = {}'.format(BASKET))

    for coinbase, bitfinex in BASKET:
        p = Process(target=lambda sym: Recorder(sym).run(), args=((coinbase, bitfinex),))
        p.start()
        logger.info('Process started up for %s' % coinbase)
        time.sleep(9)


if __name__ == "__main__":
    """
    Entry point of application
    """
    main()
