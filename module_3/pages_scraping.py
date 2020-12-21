# scraping pages entirely, saving them in .http files locally for further local requests

import pandas as pd
import asyncio
import aiohttp
import aiofiles


urls = pd.read_csv('main_task.csv', usecols=['Restaurant_id', 'URL_TA'])

semaphore = asyncio.Queue(maxsize=1000)


async def parse_page(idx, rest_id, url):
    print(idx, rest_id, url)
    endpoint = f'https://www.tripadvisor.com{url}'
    filename = f'output/{str(idx).zfill(5)}_{rest_id}_{url[1:]}'

    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint) as resp:
            text = await resp.text()
            async with aiofiles.open(filename, mode='a', encoding='utf-8') as file:
                await file.write(text)

    await semaphore.get()


async def main(cur_loop, start=0, end=urls.shape[0]):
    for idx, row in urls.iloc[start:end].iterrows():
        if not idx % 5000 and idx > 0:
            await asyncio.sleep(300)    # make 5 min pauses after each 5000 requests to avoid being banned
        await semaphore.put(idx)    # It doesn't matter what we put in the queue. We use it as semaphore.
        rest_id = row['Restaurant_id']
        url = row['URL_TA']
        cur_loop.create_task(parse_page(idx, rest_id, url))    # all the tasks are scheduled at the moment but not all done


loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop, start=i*10000, end=(i+1)*10000))
loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop=loop)))  # Wait for all tasks in the loop.
