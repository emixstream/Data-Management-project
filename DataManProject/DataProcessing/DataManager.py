from bson.objectid import ObjectId
from bson.son import SON

from Utilities import mongo_manager, analysis_thread


class DataManager:
    process_ready_tweet = [list(), list(), list(), list()]  # List of tweet Ids ready to be analyzed
    __t_selector = 0

    def __init__(self):
        self.__tweet_collection = mongo_manager.tweet_collection
        self.__btc_collection = mongo_manager.btc_collection
        self.__tps_collection = mongo_manager.tps_collection

    def save_tweet_cons(self, tweet_id):
        '''
        Consumer uses this method to distribute received tweet between thread analysis
        :param tweet_id: received id
        :return: none
        '''
        self.process_ready_tweet[self.__t_selector].append(tweet_id)
        self.__t_selector = self.__t_selector + 1
        if self.__t_selector > analysis_thread - 1:
            self.__t_selector = 0

    def aggregate_tweets_ts_id(self, ts_id):
        '''
        Query to mongo for each tweet associated to ts_id text, like , retweet
        :param ts_id: group id
        :return: collection
        '''
        pipeline = [{u"$match": {u"_id": ts_id}},
                    {u"$unwind": {u"path": u"$tweets"}},
                    {u"$group": {u"_id": u"$_id",
                                 u"elem": {u"$addToSet": {u"tweet_text": u"$tweets.tweet_text",
                                                          u"tweet_id": u"$tweets._id"}}, u"count": {u"$sum": 1.0}}}]
        return self.__tweet_collection.aggregate(pipeline)

    def drop_tweet_id(self, ts_id, tweet_id):
        """
        Drop tweet by id
        :param tweet_id: tweet id
        :param ts_id: id
        :return: none
        """
        self.__tweet_collection.update_one({u"_id": ts_id}, {u"$pull": {u"tweets": {u"_id": ObjectId(tweet_id)}}})

    def insert_tweet_text_sentiment_id(self, ts_id, tweet_id, tweet_text, sentiment):
        """
        Add to each tweet his sentiment analysis value and cleaned text used for sentiment
        :param tweet_id: tweet id
        :param tweet_text: tweet clean text
        :param sentiment: sentiment value
        :return: none
        """
        if sentiment > 0:
            classification = "pos"
        elif sentiment == 0:
            classification = "neu"
        else:
            classification = "neg"

        self.__tweet_collection.update_one({"_id": ts_id, "tweets._id": ObjectId(tweet_id)}, {
            "$set": {"tweets.$.tweet_text_cleaned": tweet_text,
                     "tweets.$.tweet_text_sentiment_classification": classification,
                     "tweets.$.tweet_text_sentiment_value": sentiment}})

    def check_btc_exists(self, ts_id):
        """
        Checks if a btc with ts_id exists
        :param ts_id: id to check
        :return: true if exists, false else
        """
        return self.__btc_collection.find({"_id": ts_id}).count() > 0

    def check_tps_exists(self, ts_id):
        """
        Checks if a tps with ts_id exists
        :param ts_id: id to check
        :return: true if exists, false else
        """
        return self.__tps_collection.find({"_id": ts_id}).count() > 0

    def get_tps_ts_id(self, ts_id):
        """
        :param ts_id: id
        :return: return document for id
        """
        return self.__tps_collection.find_one({"_id": ts_id})

    def get_btc_ts_id(self, ts_id):
        """
        :param ts_id: id
        :return: return document for id
        """
        return self.__btc_collection.find_one({"_id": ts_id})

    def integrate_sent_tps(self, ts_id, tps):
        """
        Insert the right sentiment and trans.per.second in the ts_id btc
        :param ts_id: btc id
        :param tps: transaction per sencond
        :return:
        """
        dict_sent = self.__get_count_of_sent_classification(ts_id)
        sent_mean_dict = self.__get_sentiment_mean_max_min(ts_id)
        sent_mean = sent_mean_dict["mean"]
        sent_max = sent_mean_dict["max_sent"]
        sent_min = sent_mean_dict["min_sent"]
        hashtags = self.__get_tweets_hashtags(ts_id)
        self.__btc_collection.find_one_and_update({"_id": ts_id}, {
            "$set": {"hashtags": hashtags, "sent_mean": sent_mean, "sent_min": sent_min, "sent_max": sent_max,
                     "tps": tps, "pos": dict_sent["pos"] if "pos" in dict_sent.keys() else 0,
                     "neg": dict_sent["neg"] if "neg" in dict_sent.keys() else 0,
                     "neu": dict_sent["neu"] if "neu" in dict_sent.keys() else 0}})

    def __get_count_of_sent_classification(self, ts_id):
        """
        get the count of neg pos neu tweet for a ts_id
        :param ts_id: ts_id
        :return:
        """
        pipeline = [{u"$match": {u"_id": ts_id}}, {u"$unwind": {u"path": u"$tweets"}},
                    {u"$group": {u"_id": u"$tweets.tweet_text_sentiment_classification", u"count": {u"$sum": 1.0}}}]

        result = self.__tweet_collection.aggregate(pipeline)
        res_dict = dict()
        res_dict["keys"] = list()
        for x in result:
            res_dict[x['_id']] = x['count']
            res_dict["keys"].append(x['_id'])
        return res_dict

    def __get_sentiment_mean_max_min(self, ts_id):
        """
        get the mean of sentiment for a ts_id
        :param ts_id: ts_id
        :return: min mean and max of sentiment as dict
        """
        pipeline = [{u"$match": {u"_id": ts_id}}, {u"$unwind": {u"path": u"$tweets"}},
                    {u"$group": {u"_id": u"$_id",
                               u"numerator":  {u"$sum": {u"$multiply": [u"$tweets.tweet_text_sentiment_value",
                                                        {u"$sum": [u"$tweets.tweet_favourite",1.0]}]}},
                               u"denominator": {u"$sum": {u"$sum": [u"$tweets.tweet_favourite",1.0]}},
                               u"min_sent": {u"$min": "$tweets.tweet_text_sentiment_value"},
                               u"max_sent": {u"$max": "$tweets.tweet_text_sentiment_value"}}},
                    {u"$project": {u"mean": {u"$divide": [u"$numerator", u"$denominator"]},
                                   u"min_sent": u"$min_sent",
                                   u"max_sent": u"$max_sent"}}]
        result = self.__tweet_collection.aggregate(pipeline)
        for x in result:
            return x

    def __get_tweets_hashtags(self, ts_id):
        """
        get all the hashtags contained the tweet with ts_id
        :param ts_id: ts_id
        :return: list of hashtags
        """
        pipeline = [{u"$match": {u"_id": ts_id}}, {u"$unwind": {u"path": u"$tweets"}},
                    {u"$unwind": {u"path": u"$tweets.tweet_hashtag"}},
                    {u"$group": {u"_id": u"$_id", u"hashtags": {u"$push": u"$tweets.tweet_hashtag"}}},
                    {u"$project": {u"_id": u"$_id", u"hashtags": u"$hashtags"}}]
        result = self.__tweet_collection.aggregate(pipeline)
        for x in result:
            return x["hashtags"]
