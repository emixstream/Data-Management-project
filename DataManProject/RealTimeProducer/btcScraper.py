from bs4 import BeautifulSoup
import requests
import re
import datetime
import pandas as pd
import json
import time
from selenium import webdriver
from kafka import KafkaProducer
from Utilities import parallel_run, parallel_create, chromedriver_path, chrome_binary_location


def generate_ids(time_stamp, lag_minutes=0, cut=1):
    """
    Generate ID fields from time-stamp ex. "2020-12-01 12:51:02" is "20201201125102"
    :param time_stamp: time.time()
    :param lag_minutes: if id must be generated with a lag respect to the time-stamp
    :param cut: if it is needed to remove seconds or minutes (ex. 1: 202012011251; 2: 2020120112...)
    :return: ID
    """
    tms = datetime.datetime.fromtimestamp(time_stamp)
    str_tms = tms.strftime("%Y-%m-%d %H:%M:%S")
    if lag_minutes != 0:
        date_time_obj = datetime.datetime.strptime(str_tms, "%Y-%m-%d %H:%M:%S")
        date_time_obj = date_time_obj + datetime.timedelta(minutes=lag_minutes)
        time_stamp = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
    split = re.split(" |-|:", str_tms)
    while cut > 0:
        split.pop(len(split) - 1)
        cut = cut - 1
    return "".join(split)


class ScrapeBTC:
    __topic = "btc_topic"
    __send_df = list()
    __browser_option = webdriver.ChromeOptions()
    __browser_option.binary_location = chrome_binary_location
    __browser = webdriver.Chrome(chromedriver_path, options=__browser_option)
    __producer = KafkaProducer(bootstrap_servers=["127.0.0.1:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    def __init__(self):
        self.__browser.get(f"https://coinmarketcap.com/currencies/bitcoin/")
        time.sleep(5)
        self.__manage()

    def __scrape(self):
        pages = self.__browser.page_source
        soup = BeautifulSoup(pages, 'html.parser')
        value = float(soup.find('div', "priceValue___11gHJ").contents[0].replace("$", "").replace(",", ""))
        volume_cont = soup.find('div', "hide___2JmAL statsContainer___2uXZW").contents[2]
        volume = float(volume_cont.find('div', "statsValue___2iaoZ").contents[0].replace("$", "").replace(",", ""))
        return [value, volume]

    def __manage(self):
        while True:
            res = self.__scrape()
            # GTM Hours
            t_s = datetime.datetime.fromtimestamp(time.time())
            support_ts = str(t_s).split(".")[0]
            date_time_obj = datetime.datetime.strptime(support_ts, "%Y-%m-%d %H:%M:%S")
            date_time_obj = date_time_obj - datetime.timedelta(hours=1)
            support_ts = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            send_value = dict()
            send_value["time"] = support_ts
            send_value["volume"] = res[1]
            send_value["value"] = res[0]
            print(send_value)
            self.__producer.send(topic=self.__topic, value=json.dumps(send_value))
            time.sleep(0.5999)
