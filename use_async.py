import asyncio
from cassandra import cluster

cluster.log.setLevel('INFO')

loop = asyncio.get_event_loop()


async def main():
    session = cluster.Cluster().connect()
    fut = session.execute_asyncio('SELECT now() as now FROM system.local;')
    # print(fut._response_future.result()[0])
    print(fut)

    result = await fut
    print(result)


if __name__ == '__main__':
    loop.run_until_complete(main())
