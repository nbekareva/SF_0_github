# Script intended to extract links from dataframe, write them down to .csv file for further usage for scraping

import pandas as pd

urls = pd.read_csv('main_task.csv', usecols=['URL_TA'], squeeze=True)

urls = urls.apply(lambda x: f'https://www.tripadvisor.com{x}')
print(urls)

urls.to_csv('urls.csv', index=False, header=False, sep='\n')
