import asyncio
import warnings

from cassandra import cluster

cluster.log.setLevel('INFO')

loop = asyncio.get_event_loop()

loop.set_debug(True)
loop.slow_callback_duration = 0.001
warnings.simplefilter('always', ResourceWarning)


async def main_hangs():
    session = cluster.Cluster().connect()
    fut = session.execute_asyncio('SELECT now() as now FROM system.local;')
    print('awaiting fut:', fut)
    result = await fut
    print('main: result =', result)


if __name__ == '__main__':
    # loop.run_until_complete(main())
    loop.run_until_complete(main_hangs())
    loop.close()
