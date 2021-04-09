import time
import re
from Utilities import parallel_run, parallel_create, parallel_create_w_args, printpers, analysis_thread
from DataProcessing.DataManager import DataManager
from DataProcessing.IntegrationManager import IntegrationManager
import nltk
from colorama import Fore, Style

nltk.download('stopwords')
nltk.download('vader_lexicon')
nltk.download('punkt')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nltk.download('movie_reviews')

print_topic = Fore.RED + "AnalysisManager" + Style.RESET_ALL
print_at = 10


class TweetAnalysis:
    __x_words = ["GIVEAWAY", "ETH", "WBTC", "BCH", "BSV", "XMR", "LTC", "FOLLOW",
                 "AIRDROP"]  # If a word contained in this list is found in text tweet is automatically deleted
    __rules_clean = list()  # Methods runned on tweet text before sentiment
    __rules_delete = list()  # Methods runned on tweet text if return true tweet is deleted
    __analysis_ended = list()  # List of {"ts_id": "sentiment_mean"} which are ready to be integrated
    # Timing don't care
    __general_count_processed = 0
    __general_time_used = time.time()

    def __init__(self, manager=DataManager(), integrator=IntegrationManager()):
        """
        This class will preprocess tweets before integration
        :param manager: data manager
        :param integrator: data integrator module
        """
        self.__data_manager = manager  # Data Manager
        self.__data_integrator = integrator  # Integrator module
        self.__sentiment_analyzer = SentimentIntensityAnalyzer()  # Set sentiment
        self.__stop_words = set(stopwords.words())  # Set sentiment
        self.__rules_delete = [self.__rule_find_x_words]
        self.__rules_clean = [self.__not_contain_x_char, self.__rule_remove_stop_words]  # Add cleaning text rules
        for t in range(0, analysis_thread):  # Create thread for tweet analysis for settings look at data manager
            self.__t_analysis = parallel_create_w_args(self.__start_analysis, t)
            parallel_run(self.__t_analysis)
        self.__t_move_processed = parallel_create(
            self.__process_ready_tweet)  # Start thread which will communicate with integrator
        parallel_run(self.__t_move_processed)

    def __rule_find_x_words(self, tweet_text):
        """
        This method will look for word which would make the tweet deleted
        :param tweet_text: tweet text
        :return: False if find a word True else
        """
        for x in self.__x_words:
            if x in tweet_text:
                return False
        return True

    def __rule_remove_stop_words(self, tweet_text):
        """
        This method will remove stopwords from tweet text
        :param tweet_text: tweet text
        :return: cleaned text
        """
        word_tokens = word_tokenize(tweet_text)
        filtered_sentence = [w for w in word_tokens if not w in self.__stop_words]
        filtered_sentence = ' '.join(filtered_sentence)
        return filtered_sentence

    def __not_contain_x_char(self, tweet_text):
        """
        This method will remove unwanted char from tweet text
        :param tweet_text: tweet text
        :return: cleaned text
        """
        ret_value = re.sub(" \w+@\w+.\w+", "", tweet_text)
        ret_value = re.sub("http\S+", "", ret_value)
        ret_value = re.sub("[0-9]*", "", ret_value)
        ret_value = re.sub("(â|â|-|\+|`|#|,|;|)*", "", ret_value)
        return re.sub("@\w+", "", ret_value)

    def __start_analysis(self, t_selector):
        """
        This method would start tweet analysis
        :param t_selector: Thread number, would select id related to right thread
        :return:
        """
        self.__general_time_used = time.time()
        while True:
            while len(self.__data_manager.process_ready_tweet[t_selector]) == 0:  # Check id list is not empty
                if analysis_thread > 2:
                    time.sleep(5)
                continue
            id = self.__data_manager.process_ready_tweet[t_selector][0]
            self.__data_manager.process_ready_tweet[t_selector].pop(0)
            key = list(id.keys())[0]
            ids = self.__data_manager.aggregate_tweets_ts_id(key)  # Check tweets with this id
            if ids is None:  # -are loaded on mongo
                self.__data_manager.process_ready_tweet[t_selector].append(id)
                continue
            for elements in ids:
                x = int(elements["count"])  # num. of tweets on mongo related to this id
                y = int(list(id.values())[0])  # num. of tweet should be on mongo
                if x != y:
                    self.__data_manager.process_ready_tweet[t_selector].insert(0, id)
                    break
                else:
                    for tweet in elements['elem']:  # calculate weighted sentiment on like
                        tweet_id = tweet["tweet_id"]
                        tweet_text = tweet["tweet_text"]
                        if tweet_text is None:
                            self.__data_manager.drop_tweet_id(ts_id=key, tweet_id=tweet_id)
                            self.__general_count_processed = self.__general_count_processed + 1
                            continue
                        self.__start_analysis_single(key, tweet_id, tweet_text)  # calculate sentiment and execute rule
                        # Timing don't care
                        if self.__general_count_processed % print_at == 0:
                            time_int = time.time() - self.__general_time_used
                            printpers(print_topic,
                                      "Analyzed : " + str(self.__general_count_processed) + " :: Time : " + str(
                                          time_int))
                    self.__analysis_ended.append(key)  # Add processed ts_id to list

    def __start_analysis_single(self, ts_id, id, text):
        for rule in self.__rules_delete:
            if not rule(text.capitalize()):
                self.__data_manager.drop_tweet_id(ts_id=ts_id, tweet_id=id)
                self.__general_count_processed = self.__general_count_processed + 1
                return
        text_clean = text.capitalize()
        for rule in self.__rules_clean:
            text_clean = rule(text_clean)
        sentiment = self.__sentiment_analyzer.polarity_scores(text_clean)["compound"]
        self.__data_manager.insert_tweet_text_sentiment_id(ts_id, id, text_clean, sentiment)
        self.__general_count_processed = self.__general_count_processed + 1

    def __process_ready_tweet(self):
        while True:
            if len(self.__analysis_ended) == 0:
                time.sleep(10)
                continue
            add_v = self.__analysis_ended.pop(0)
            self.__data_integrator.add_id(add_v)
