import pandas as pd
from lxml import html
import asyncio
import aiohttp
# import nest_asyncio
# nest_asyncio.apply()

urls = pd.read_csv('main_task.csv', usecols=['URL_TA'], squeeze=True)

parsed_data = pd.DataFrame(columns=['Cuisine Style', 'Price Range', 'Number of Reviews'])


async def parse_3cols_from_TA(idx, url):
    print(idx, url, 'test')
    endpoint = f'https://www.tripadvisor.com{url}'

    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint) as resp:
            text = await resp.text()
            tree = html.fromstring(text)
            current_cuisines = tree.xpath(
                '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[3]/a/text()')  # price range and cuisine styles
            current_cuisines = [word for word in current_cuisines if
                                '$' not in word]  # eliminating price range if parsed

            current_price_range = tree.xpath(
                '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[3]/a[1]/text()')  # price range
            current_price_range = str(current_price_range[0]) if (len(current_price_range) > 0 and
                                                                  '$' in current_price_range[0]) else None

            current_num = tree.xpath(
                '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[1]/a/span/text()[1]')  # number of reviews
            current_num = int(current_num[0].replace(',', '')) if len(current_num) > 0 else None

            # print(current_cuisines, type(current_cuisines),
            #       current_price_range, type(current_price_range)
            #       current_num, type(current_num))

        await session.close()

    parsed_data.loc[idx, ['Cuisine Style', 'Price Range', 'Number of Reviews']] = [
        current_cuisines or None, current_price_range, current_num
    ]


async def main():
    for i in range(0, urls.shape[0], 500):  # limiting async functions running by 500 at a time
        await asyncio.gather(*[parse_3cols_from_TA(idx, url) for idx, url in urls.iloc[i:i + 500].items()])


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
print(parsed_data)
parsed_data.to_csv('parsed_data_output.csv')