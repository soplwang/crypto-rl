import asyncio
from threading import Thread
from data_recorder.coinbase_connector.coinbase_client import CoinbaseClient


if __name__ == "__main__":
    """
    This __main__ function is used for testing the
    CoinbaseClient class in isolation.
    """

    loop = asyncio.get_event_loop()
    symbols = ['BTC-USD']  # , 'BCH-USD', 'LTC-USD', 'ETH-USD']
    p = dict()

    print('Initializing...%s' % symbols)
    for sym in symbols:
        p[sym] = CoinbaseClient(sym)

    threads = [Thread(target=lambda sym: p[sym].run(),
                        args=(sym,),
                        name=sym, daemon=True) for sym in symbols]
    [thread.start() for thread in threads]

    tasks = asyncio.gather(*[(p[sym].subscribe()) for sym in symbols])
    print('Gathered %i tasks' % len(symbols))

    try:
        loop.run_until_complete(tasks)
        print('TASK are complete for {}'.format(symbols))
        loop.close()
        [thread.join() for thread in threads]
        print('loop closed.')

    except KeyboardInterrupt as e:
        print("Caught keyboard interrupt. Canceling tasks...")
        tasks.cancel()
        loop.close()
        [thread.join() for thread in threads]
        
    finally:
        loop.close()
        print('\nFinally done.')
