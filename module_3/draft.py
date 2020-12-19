import requests
import json
import pandas as pd


# countries info
response = requests.get('https://restcountries.eu/rest/v2/region/europe')

with open('country_data.json', 'w') as outfile:
    json.dump(response.json(), outfile)


# currency rates on 2020-01-01
url = "https://fixer-fixer-currency-v1.p.rapidapi.com/2020-01-01"
querystring = {"base": "EUR",
               "symbols": "EUR, ALL, BYN, BAM, BGN, HRK, CZK, DKK, GIP, GBP, \
               HUF, ISK, CHF, MKD, MDL, NOK, PLN, RON, RUB, RSD, SEK, UAH"}
headers = {
    'x-rapidapi-key': "0d5bc9eba2mshe3958be9118d540p1c5254jsn1c94cb29d026",
    'x-rapidapi-host': "fixer-fixer-currency-v1.p.rapidapi.com"
    }
response = requests.request("GET", url, headers=headers, params=querystring)

with open('currency_rates.json', 'w') as outfile:
    json.dump(response.json(), outfile)


# cities info, taken from http://data.un.org/Data.aspx?d=POP&f=tableCode:240
data = pd.read_csv('UNdata_Export.csv')
data = data.drop(['Value Footnotes'], axis=1)

idx = data[data['Country or Area'] == 'footnoteSeqID'].index[0]
data = data.iloc[:idx]

countries = {
    '2019': ['Austria', 'Luxembourg', 'Norway'],
    '2018': ['Denmark', 'Czechia', 'Germany', 'Hungary', 'Poland', 'Portugal', 'Slovakia', 'Slovenia', 'Spain'],
    '2017': ['Finland'],
    '2016': ['Ireland'],
    '2015': ['France'],
    '2012': ['Holy See'],
    '2011': ['Belgium', 'Greece', 'United Kingdom of Great Britain and Northern Ireland'],
    '2007': ['Sweden']
}

for year, set_i in countries.items():
    for country in set_i:
        data = data[((data['Country or Area'] == country) & (data['Year'] == year)) | (data['Country or Area'] != country)]

data.drop_duplicates(subset=['Country or Area', 'City', 'Sex'], inplace=True)
data.reset_index(drop=True, inplace=True)

data.to_csv('UNdata_Export_new.csv', index=False)
