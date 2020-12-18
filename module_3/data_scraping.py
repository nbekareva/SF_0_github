import pandas as pd
from lxml import html
import asyncio
import aiohttp


urls = pd.read_csv('main_task.csv', usecols=['Restaurant_id', 'URL_TA'])

parsed_data = pd.DataFrame(columns=['Restaurant_id', 'Cuisine Style', 'Price Range', 'Number of Reviews'])

semaphore = asyncio.Queue(maxsize=1000)


async def parse_from_TA(idx, rest_id, url):
    print(idx, rest_id, url)
    endpoint = f'https://www.tripadvisor.com{url}'

    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint) as resp:
            text = await resp.text()
            tree = html.fromstring(text)

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

    parsed_data.loc[idx, ['Restaurant_id', 'Cuisine Style', 'Price Range', 'Number of Reviews']] = [
        rest_id, current_cuisines or None, current_price_range, current_num
    ]
    await semaphore.get()


async def main(loop, start=0, end=urls.shape[0]):
    for idx, row in urls.iloc[start:end].iterrows():
        if not idx % 5000 and idx > 0:
            await asyncio.sleep(300)    # make 5 min pauses after each 5000 requests to avoid being banned
        await semaphore.put(idx)    # It doesn't matter what we put in the queue. We use it as semaphore.
        rest_id = row['Restaurant_id']
        url = row['URL_TA']
        loop.create_task(parse_from_TA(idx, rest_id, url))    # all the tasks are scheduled at the moment but not all done


for i in range(urls.shape[0]//10000):    # processing only by 10000 tasks at a time, writing results in files
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, start=i*10000, end=(i+1)*10000))
    loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop=loop)))  # Wait for all tasks in the loop.
    parsed_data.to_csv(f'parsed_data_output{i}.csv')
