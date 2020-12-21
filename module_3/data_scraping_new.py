# scraping from .html local files.
# (!) Consider rewriting the code using threading

import pandas as pd
from lxml import html
import json
import asyncio
import aiofiles


urls = pd.read_csv('main_task.csv', usecols=['Restaurant_id', 'URL_TA'])

parsed_dict = {}

semaphore = asyncio.Queue(maxsize=1000)


async def parse_from_file(idx, rest_id, url):
    print(idx, rest_id, url)
    filename = f'output/{str(idx).zfill(5)}_{rest_id}_{url[1:]}'

    async with aiofiles.open(filename, mode='r', encoding='utf-8') as file:
        page = await file.read()
        tree = html.fromstring(page)

        current_cuisines = tree.xpath(
            '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[3]/a/text()',
            smart_strings=False)  # price range and cuisine styles
        current_cuisines = [word for word in current_cuisines if
                            '$' not in word]  # eliminating price range if parsed

        current_price_range = tree.xpath(
            '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[3]/a[1]/text()',
            smart_strings=False)  # price range
        current_price_range = current_price_range[0] if (len(current_price_range) > 0 and
                                                         '$' in current_price_range[0]) else None

        current_num = tree.xpath(
            '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[1]/a/span/text()[1]',
            smart_strings=False)  # number of reviews
        current_num = int(current_num[0].replace(',', '')) if len(current_num) > 0 else None

        parsed_dict[idx] = {
            'Restaurant_id': rest_id,
            'Cuisine Style': current_cuisines or None,
            'Price Range': current_price_range,
            'Number of Reviews': current_num,
            'URL_TA': url
        }

    await semaphore.get()


async def main(loop, start=0, end=urls.shape[0]):
    for idx, row in urls.iloc[start:end].iterrows():
        await semaphore.put(idx)    # It doesn't matter what we put in the queue. We use it as semaphore.
        rest_id = row['Restaurant_id']
        url = row['URL_TA']
        loop.create_task(parse_from_file(idx, rest_id, url))    # all the tasks are scheduled at the moment but not all done


for i in range(urls.shape[0]//10000):    # processing only by 10000 tasks at a time, writing results in files, for safety
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, start=i*10000, end=(i+1)*10000))
    loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop=loop)))  # Wait for all tasks in the loop.

    with open(f'parsed_data_output{i}.json', 'w') as outfile:
        json.dump(parsed_dict, outfile)
