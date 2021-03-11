from main import *
import xlrd
import lxml
#import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

logging.getLogger().setLevel(logging.INFO)






def read_df_from_html(html, match=None):
    logging.debug("read_df_from_html html length={}, match={}".format(len(html), match))
    df = pd.DataFrame()
    df_list = pd.read_html(html, match)
    if len(df_list) == 1:
        df = df_list[0]
    return df


def test1(file):
    df = pd.read_html(file)[0]
    print(df.head(10))
    print(df.columns[1])
    s = df\
        .rename(columns={df.columns[1]: 'Date'})\
        .to_csv(index=False, sep=';')
    #book = xlrd.open_workbook(file)
    #print("The number of worksheets is {}".format(book.nsheets))
    print(s)


def test2(url):
    logging.info('{}'.format(url))
    #f = open(file, 'r')
    #s = f.read()
    s = requests.get(url).content
    hist_table_from_html(s,2020)
    #return parser(s,skip_rows)


def test3(url,match):
    logging.info('{}'.format(url))
    s = requests.get(url).content
    print(read_df_from_html(s,match).to_csv())


print(test2('https://en.wikipedia.org/wiki/Cost_of_electricity_by_source'))
#print(test3('https://en.wikipedia.org/wiki/Cost_of_electricity_by_source','German'))