from kafka import KafkaProducer
import json
import pandas as pd
from threading import Thread
import time
from ast import literal_eval
from Utilities import path_to_tweet_csv, path_to_btc_csv, path_to_tps_csv


class ProducerUniv:
    __path_to_btc = path_to_btc_csv
    __path_to_tweet = path_to_tweet_csv
    __path_to_tps = path_to_tps_csv
    __topic = ""
    __reading_tweet = list()

    def __init__(self, producer_type):
        self.__path_to_csv = ""
        self.__topic = ""
        self.__producer = KafkaProducer(bootstrap_servers=["127.0.0.1:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        if producer_type == "tweet":
            self.__path_to_csv = self.__path_to_tweet
            self.__topic = "tweet_topic"
            self.__run = self.__prepare_file_tweet
            self.__run()
        elif producer_type == "btc":
            self.__path_to_csv = self.__path_to_btc
            self.__topic = "btc_topic"
            self.__run = self.__prepare_file_btc
            self.__run()
        elif producer_type == "tps":
            self.__path_to_csv = self.__path_to_tps
            self.__topic = "tps_topic"
            self.__run = self.__prepare_file_tps
            self.__run()
        elif producer_type == "analysis":
            self.__topic = "analysis_topic"
            self.run = self.__send
        else:
            return

    def __prepare_file_btc(self):
        df1 = pd.read_csv(self.__path_to_csv)
        self.__send_file(df=df1)

    def __prepare_file_tps(self):
        df = pd.read_csv(self.__path_to_csv)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        df = df[(df["Timestamp"].dt.month > 10) & (df["Timestamp"].dt.month < 12)]
        df.rename(columns={'Timestamp': 'time'}, inplace=True)
        self.__send_file(df)

    def __prepare_file_tweet(self):
        df1 = pd.read_csv(self.__path_to_csv)
        df1.rename(columns={'tweet_date': 'time'}, inplace=True)
        df1["tweet_hashtag"] = df1["tweet_hashtag"].apply(lambda x: literal_eval(x))
        self.__send_file(df=df1)

    def __send_file(self, df):
        df = df.sort_values(by="time")
        df["time"] = df["time"].astype(str)
        json_to_send = list(df.apply(lambda row: row.to_json(), axis=1))
        print(len(json_to_send))
        count = 0
        for x in json_to_send:
            self.__producer.send(topic=self.__topic, value=x)
            count = count + 1
            if self.__topic == "tweet_topic":
                print((json.loads(x))["time"])
            if self.__topic == "tweet_topic" and (count % 10000 == 0):
                print("sent : ", str(count), " tweet")

    def __send(self):
        while True:
            if not (len(self.__reading_tweet) > 0):
                time.sleep(20)
                continue
            tweet_id = self.__reading_tweet.pop(0)
            self.__producer.send(topic=self.__topic, value=json.dumps(tweet_id))

    def add_tweet_ts_id_count(self, ts_id, count_per_id):
        self.__reading_tweet.append({ts_id: count_per_id})

    def add_tweet_ts_id(self, ts_id):
        self.__reading_tweet.append(ts_id)


def main():
    proc3 = Thread(target=ProducerUniv, args=("tweet",))
    proc3.start()
    proc4 = Thread(target=ProducerUniv, args=("btc",))
    proc4.start()
    proc5 = Thread(target=ProducerUniv, args=("tps",))
    proc5.start()
    proc3.join()
    while True:
        time.sleep(3600)


if __name__ == '__main__':
    main()
