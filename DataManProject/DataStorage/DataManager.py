import json
from Utilities import parallel_run, parallel_create, mongo_manager, is_realtime, printpers
from Producer.ProducerHD import ProducerUniv
import re
from bson import ObjectId
import datetime
from colorama import Fore, Style

print_at = 100
print_topic = Fore.YELLOW + "ReceiverManager" + Style.RESET_ALL


class DataManager:
    __rec_tweets = list()  # This list saves the received tweets
    __rec_btc_data = list()  # This list saves the received btc
    __rec_tps_data = list()  # This list saves the received tps

    __added_tweets = False  # Boolean value used to indicate if the tread which manages the save of tweet is alive or
    # not
    __added_btc = False  # Boolean value used to indicate if the tread which manages the save of btc is alive or not
    __added_tps = False  # Boolean value used to indicate if the tread which manages the save of tps is alive or not

    __actual_ts_tweet = ""  # tweets received are grouped by a timestamp id (crated at self __generate_ids) here we
    # save the actual processing timestamp
    __actual_count_tweet = 0  # number of tweets received associated with actual_ts

    __actual_ts_btc = ""  # btc received are grouped by a timestamp id (crated at self __generate_ids) here we
    # save the actual processing timestamp

    __actual_ts_tps = ""  # tps received are grouped by a timestamp id (crated at self __generate_ids) here we
    # save the actual processing timestamp

    __general_count_tweet = 0 #IGNORE
    __general_count_btc = 0 #IGNORE
    __general_count_tps = 0 #IGNORE

    def __init__(self):
        self.__tweet_collection = mongo_manager.tweet_collection
        self.__btc_collection = mongo_manager.btc_collection
        self.__tps_collection = mongo_manager.tps_collection
        self.__btc_sup_collection = mongo_manager.btc_sup_collection
        self.__tps_sup_collection = mongo_manager.tps_sup_collection
        self.__btc_collection.delete_many({})
        self.__tweet_collection.delete_many({})
        self.__tps_collection.delete_many({})
        self.__tps_sup_collection.delete_many({})
        self.__btc_sup_collection.delete_many({})

        if not is_realtime:
            self.__save_btc = self.__save_btc_not_realtime
            self.__save_tps = self.__save_tps_not_realtime
        else:
            self.__save_btc = self.__save_btc_realtime
            self.__save_tps = self.__save_tps_realtime

        # Thread which would manage the tweet and btc save to mongodb
        self.__t_tweet = parallel_create(self.__save_tweet)
        self.__t_btc = parallel_create(self.__save_btc)
        self.__t_tps = parallel_create(self.__save_tps)

        # Thread which would inform DataProcessing module if files are send to mongo
        self.__producer_send_tweet = ProducerUniv("analysis")
        self.__t_producer_send_tweet = parallel_create(self.__producer_send_tweet.run)

    def start_int_producer(self):
        if not self.__t_producer_send_tweet.is_alive():
            parallel_run(self.__t_producer_send_tweet)

    def add_tweet(self, tweet):
        """
        Adds each new tweet received by the consumer to the tweet list
        :param tweet: tweet to add
        """
        self.__rec_tweets.append(tweet.value)
        if not self.__added_tweets:  # The above code starts tweet thread(t_tweet) if it is stopped
            self.__added_tweets = True
            if not self.__t_tweet.is_alive():
                parallel_run(self.__t_tweet)

    def add_btc(self, btc):
        """
        Adds each new btc value received by the consumer to the btc list
        :param btc: btc to add
        """
        self.__rec_btc_data.append(btc.value)
        if not self.__added_btc:  # The above code starts btc thread(t_btc) if it is stopped
            self.__added_btc = True
            if not self.__t_btc.is_alive():
                parallel_run(self.__t_btc)

    def add_tps(self, tps):
        """
        Adds each new btc value received by the consumer to the btc list
        :param tps: tps to add
        """
        self.__rec_tps_data.append(tps.value)
        if not self.__added_tps:  # The above code starts btc thread(t_btc) if it is stopped
            self.__added_tps = True
            if not self.__t_tps.is_alive():
                parallel_run(self.__t_tps)

    def __save_tweet(self):
        """
        Executed by thread will save each tweet to mongodb
        """
        while self.__added_tweets:
            tweet = json.loads(self.__rec_tweets[0])
            self.__rec_tweets.pop(0)
            key = self.__generate_ids(tweet["time"], lag=2)

            if self.__actual_ts_tweet == "":  # instantiate actual analysing ts_id with first ts_id
                self.__actual_ts_tweet = key

            if key != self.__actual_ts_tweet:  # if the actual analysing tweet change we send the older
                self.__producer_send_tweet.add_tweet_ts_id_count(self.__actual_ts_tweet, self.__actual_count_tweet)
                self.__actual_ts_tweet = key
                self.__actual_count_tweet = 0

            tweet["_id"] = ObjectId()
            self.__tweet_collection.update({"_id": key}, {"$push": {"tweets": tweet}}, upsert = True)
            self.__actual_count_tweet = self.__actual_count_tweet + 1

            self.__general_count_tweet = self.__general_count_tweet + 1
            if self.__general_count_tweet % print_at == 0:
                printpers(print_topic + " : TWEET", "Received : " + str(self.__general_count_tweet))

            if len(self.__rec_tweets) == 0:  # if there are no more tweet to save thread would be killed
                self.__added_tweets = False
                self.__t_tweet = parallel_create(self.__save_tweet)

    def __save_btc_realtime(self):
        """
        Executed by thread will save each btc to mongodb
        """
        while self.__added_btc:
            tmp_data = json.loads(self.__rec_btc_data[0])
            self.__rec_btc_data.pop(0)
            btc_id = (self.__generate_ids(tmp_data["time"], lag=0))
            if self.__actual_ts_btc != btc_id:
                self.__transport_btc_realtime(self.__actual_ts_btc)
            self.__actual_ts_btc = btc_id
            self.__btc_sup_collection.update({"_id": btc_id}, {"$push":{"btc": tmp_data}}, upsert= True)

            #Timing don't care ---------
            self.__general_count_btc = self.__general_count_btc + 1
            if self.__general_count_btc % 1000 == 0:
                printpers(print_topic + " : BTC", "Received : " + str(self.__general_count_btc))
            # --------
            if len(self.__rec_btc_data) == 0:  # if there are no more tweet to save thread would be killed
                self.__added_btc = False
                self.__t_btc = parallel_create(self.__save_btc)

    def __transport_btc_realtime(self, ts_id):
        pipeline = [{u"$match": {u"_id": ts_id}}, {u"$unwind": {u"path": u"$btc"}}, {
            u"$group": {u"_id": u"$_id", u"time": { u"$first": u"$btc.time"}, u"open": {u"$first": u"$btc.value"}, u"close": {u"$last": u"$btc.value"},u"high": {u"$max": u"$btc.value"}, u"low": {u"$min": u"$btc.value"}, u"btc_volume": {u"$first": u"$btc.volume"}}}]
        res = self.__btc_sup_collection.aggregate(pipeline)
        for x in res:
            self.__btc_collection.insert_one(x)


    def __save_tps_realtime(self):
        """
        Executed by thread will save each tps to mongodb
        """
        while self.__added_tps:
            tmp_data = json.loads(self.__rec_tps_data[0])
            self.__rec_tps_data.pop(0)
            tps_id = (self.__generate_ids(tmp_data["time"], lag=0))
            if self.__actual_ts_tps != tps_id:
                self.__transport_tps_realtime(self.__actual_ts_tps)
            self.__actual_ts_tps = tps_id
            self.__tps_sup_collection.update({"_id": self.__actual_ts_tps}, {"$push": {"tps": tmp_data}}, upsert = True)

            # Timing don't care ------
            self.__general_count_tps = self.__general_count_tps + 1
            if self.__general_count_tps % 1000 == 0:
                printpers(print_topic + " : TPS", "Received : " + str(self.__general_count_tps))
            #-----------
            if len(self.__rec_tps_data) == 0:  # if there are no more tweet to save thread would be killed
                self.__t_tps = parallel_create(self.__save_tps)
                self.__added_tps = False

    def __transport_tps_realtime(self, ts_id):
        pipeline = [{u"$match": {u"_id": ts_id}}, {u"$unwind": {u"path": u"$tps"}},
            {u"$group": {u"_id": u"$_id", u"time": {u"$first": u"$tps.time"}, u"value": {u"$avg": u"$tps.value"}}}]
        res = self.__tps_sup_collection.aggregate(pipeline)
        for x in res:
            self.__tps_collection.insert_one(x)

    def __save_btc_not_realtime(self):
        """
        Executed by thread will save each btc to mongodb
        """
        while self.__added_btc:
            saving_data = json.loads(self.__rec_btc_data[0])
            self.__rec_btc_data.pop(0)
            saving_data["_id"] = (self.__generate_ids(saving_data["time"], lag=0))
            saving_data["volume_x"] = saving_data.pop("volume")
            self.__btc_collection.insert_one(saving_data)
            # Timing don't care ------
            self.__general_count_btc = self.__general_count_btc + 1
            if self.__general_count_btc % 1000 == 0:
                printpers(print_topic + " : BTC", "Received : " + str(self.__general_count_btc))
            # ------
            if len(self.__rec_btc_data) == 0:  # if there are no more tweet to save thread would be killed
                self.__added_btc = False
                self.__t_btc = parallel_create(self.__save_btc)

    def __save_tps_not_realtime(self):
        """
        Executed by thread will save each transaction record to mongodb
        """
        while self.__added_tps:
            saving_data = json.loads(self.__rec_tps_data[0])
            self.__rec_tps_data.pop(0)
            saving_data["_id"] = (self.__generate_ids(saving_data["time"]))
            self.__tps_collection.insert_one(saving_data)

            # Timing don't care ------
            self.__general_count_tps = self.__general_count_tps + 1
            if self.__general_count_tps % 1000 == 0:
                printpers(print_topic + " : TPS", "Received : " + str(self.__general_count_tps))
            # --------
            if len(self.__rec_tps_data) == 0:  # if there are no more tweet to save thread would be killed
                self.__t_tps = parallel_create(self.__save_tps)
                self.__added_tps = False

    def __generate_ids(self, time_stamp, lag=0, cut=1):
        """
        Generate ID from time-stamp ex. "2020-12-01 12:51:02" is "20201201125102"
        :param time_stamp: time-stamp
        :param lag: if id must be generated with a lag respect to the time-stamp
        :param cut: if it is needed to remove seconds or minutes (ex. 1: 202012011251; 2: 2020120112...)
        :return: ID
        """
        if lag != 0:
            date_time_obj = datetime.datetime.strptime(time_stamp, "%Y-%m-%d %H:%M:%S")
            date_time_obj = date_time_obj + datetime.timedelta(minutes=lag)
            time_stamp = date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
        split = re.split(" |-|:", time_stamp)
        while cut > 0:
            split.pop(len(split) - 1)
            cut = cut - 1
        return "".join(split)
