import pandas as pd
from lxml import html
import asyncio
import aiohttp
# import nest_asyncio
# nest_asyncio.apply()

urls = pd.read_csv('main_task.csv', usecols=['URL_TA'], squeeze=True)
urls = urls.iloc[:10000]

parsed_data = pd.DataFrame(columns=['Cuisine Style', 'Price Range', 'Number of Reviews'])

semaphore = asyncio.Queue(maxsize=1000)


def replace_chars(text):
    """
    Is needed due to html.fromstring() memory allocation problems when stumbling up on HTML Control Characters.
    Memory gets allocated but never cleaned up, so the script crashes.
    The function eliminates these characters from the http response text.
    :param text:
    :return text:
    """
    chars = ['&emsp;', '&ensp;', '&nbsp;', '&#09;', '&#10;', '&#32;']
    for char in chars:
        text = text.replace(char, '')
    return text


async def parse_3cols_from_TA(idx, url):
    print(idx, url, 'test')
    endpoint = f'https://www.tripadvisor.com{url}'

    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint) as resp:
            text = await resp.text()
            tree = html.fromstring(replace_chars(text))
            current_cuisines = tree.xpath(
                '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[3]/a/text()', smart_strings=False)  # price range and cuisine styles
            current_cuisines = [word for word in current_cuisines if
                                '$' not in word]  # eliminating price range if parsed
            # for word in current_cuisines:
            #     print('cuisine', word, type(word))

            current_price_range = tree.xpath(
                '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[3]/a[1]/text()', smart_strings=False)  # price range
            current_price_range = current_price_range[0] if (len(current_price_range) > 0 and
                                                                  '$' in current_price_range[0]) else None
            # print('cur price', current_price_range, type(current_price_range))

            current_num = tree.xpath(
                '//*[@id="taplc_top_info_0"]/div/div/div[2]/span[1]/a/span/text()[1]', smart_strings=False)  # number of reviews
            current_num = int(current_num[0].replace(',', '')) if len(current_num) > 0 else None
            # print('cur num', current_num, type(current_num))

            # print(idx, current_cuisines, type(current_cuisines),
            #      current_price_range, type(current_price_range),
            #      current_num, type(current_num))

        # await session.close()
    await semaphore.get()

    parsed_data.loc[idx, ['Cuisine Style', 'Price Range', 'Number of Reviews']] = [
        current_cuisines or None, current_price_range, current_num
    ]


# async def main():
#     for i in range(0, urls.shape[0], 500):  # limiting async functions running by 500 at a time
#
#         print(i + 500)
#         await asyncio.gather(*[parse_3cols_from_TA(idx, url) for idx, url in urls.iloc[i:i + 500].items()])
#
#
# urls = pd.read_csv('main_task.csv', usecols=['URL_TA'], squeeze=True)
#
# parsed_data = pd.DataFrame(columns=['Cuisine Style', 'Price Range', 'Number of Reviews'])
#
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
# loop.close()
# print(parsed_data)
# parsed_data.to_csv('parsed_data_output.csv')


async def main(loop):
    for idx, url in urls.items():
        await semaphore.put(idx)    # It doesn't matter what we put in the queue. We use it as semaphore.
        loop.create_task(parse_3cols_from_TA(idx, url))    # all the tasks are scheduled at the moment but not all done


loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
loop.run_until_complete(asyncio.gather(*asyncio.all_tasks()))    # Wait for all tasks in the loop.

# loop.close()
# print(parsed_data)
parsed_data.to_csv('parsed_data_output.csv')