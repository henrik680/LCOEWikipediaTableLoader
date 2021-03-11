import pandas as pd
from bs4 import BeautifulSoup
import logging
import argparse
import json
from urllib.request import urlopen
from TableFormatterRegion import *
from RenewalyticsGcpStorageLib import * ### Python package dependency google-cloud-storage

logging.getLogger().setLevel(logging.INFO)
project_name = 'LCOEWikipediaTableLoader'


#*****#####################################################################
# Pandas Dataframe
#
#

def dataframe_from_html(html):
    df = pd.DataFrame()
    df_list = pd.read_html(str(html))
    if len(df_list) == 1:
        df = df_list[0]
        logging.debug(df)
    return df


#*****#####################################################################
# HTML
#
#


def table_caption_from_html_table(table):
    captions = table.find_all('caption')
    if len(captions) == 1:
        return captions[0].get_text()
    else:
        return "No caption"

#*****#####################################################################


def table_dict_from_html(html):
    logging.debug("Started table_dict_from_html")
    soup = BeautifulSoup(html, "lxml")      # "html.parser" / "lxml"
    tables = soup.findAll('table')
    table_dict = {}
    for table_html in tables:
        c = table_caption_from_html_table(table_html)
        if c != 'No caption':
            length = len(table_html.findAll('tr'))
            logging.debug("length={}".format(length))
            table_dict[c] = table_html
    logging.debug("table_dict_from_html will return {}".format(table_dict.keys()))
    return table_dict


def run(request):
    logging.info("Starting {}".format(project_name))
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', help='json with parameters')
    parser.add_argument('--metadata', help='json with parameters')
    args = parser.parse_args()
    if request is None:
        f = open('argfile.txt','r')
        input_json = json.loads(str(f.read()).replace("'", ""))
    else:
        input_json = request.get_json()
    logging.info("request.args: {}".format(input_json))
    bucket_name = input_json['bucket_target']
    url = input_json['data_path_url']
    metadata = json.loads(input_json['metadata'])
    metadata['data_path_url'] = url
    destination_blob_name = input_json['destination_blob_name']
    logging.info("\nbucket_name: {}\nmetadata: {}\nurl: {}\ndestination_blob_name: {}".format(
       bucket_name, metadata, url, destination_blob_name))

    table_dict = table_dict_from_html(urlopen(url).read())
    regions = [r.strip() for r in metadata['regions'].split(',')]
    logging.debug("Regions from input data: {}".format(regions))
    for region in regions:
        res = [i for i in table_dict.keys() if region in i]
        if len(res) == 1:
            table_caption = res[0]
            logging.debug("Found table: caption={} and size={}".format(table_caption,table_dict[table_caption].size))
            df = dataframe_from_html(table_dict[table_caption])
            if region == 'EIA':
                df = reformat_EIA(df)
            csv_string = df.to_csv(index=False, sep=';')
            metadata['region'] = region
            upload_blob_string(bucket_name, csv_string, destination_blob_name+region.replace(' ', '').strip(), metadata)
            logging.info("Uploaded size={} to bucket {} and {}".format(df.size, bucket_name, destination_blob_name))


if __name__ == '__main__':
    run(None)

