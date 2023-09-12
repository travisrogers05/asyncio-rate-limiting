#!/usr/bin/env python

import asyncio
import logging
import time
import aiohttp
import attr
from attr.validators import instance_of

LOGGER_FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)

@attr.s
class Fetch:

    limit = attr.ib()
    rate = attr.ib(default = 0.0, converter = float)
    retrievedUrls = {}

    async def make_request(self, url):
        async with self.limit:
            async with aiohttp.ClientSession() as session:
                for i in range(3):
                    try:
                        async with session.request(method = 'GET', url = url) as response:
                            json = await response.json()
                            status = response.status
                            self.retrievedUrls[int(url.split('/')[-1])] = json['url']
                        break
                    except aiohttp.client_exceptions.ClientOSError as e:
                        print(f'{e}')
                        await asyncio.sleep(1.0*self.rate)
                    except Exception as e:
                        print(f'{e}')
                        await asyncio.sleep(1.0*self.rate)
                await asyncio.sleep(self.rate)


async def runner(urls, rate, limit):
    limit = asyncio.Semaphore(limit)
    f = Fetch(limit=limit, rate=rate)
    print(f'f = {f}')

    tasks = []
    for url in urls:
        tasks.append(f.make_request(url = url))
    
    results = await asyncio.gather(*tasks)

    print(f'Total requested URLs {len(f.retrievedUrls)}')

    for k,v in sorted(f.retrievedUrls.items()):
        if k != int(v.split('/')[-1]):
            print(f'key: {k}, value: {v}')





URL_LIMIT = 5000
urls = [
    # f'http://httpbin.org/anything/{n}' for n in range(URL_LIMIT)
    f'http://localhost:8080/anything/{n}' for n in range(URL_LIMIT)
]
LIMIT = 28
RATE = 0.01
start = time.perf_counter()
asyncio.run(
    runner(
        urls = urls,
        rate = RATE,
        limit = LIMIT
    )
)
end = time.perf_counter()
print(f'Total runtime {round(end - start,1)} seconds')
