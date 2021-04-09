from kafka import KafkaConsumer
from Utilities import printpers
import json
import time

print_topic = "Consumer"
class ConsumerUniv:
    def __init__(self, topic, manager):
        """
        This class instantiate a consumer, valid both for tweet and btc
        :param topic: "btc" or "tweet" or "analysis" or "tps"
        :param manager: DataReceivedManager, instance of class which would manage data received by consumer
        """
        self.__topic = ""
        # This class is valid for more consumer type so on init would be instantiated the needed component basing on topic
        if topic == "tweet":
            self.run = self.__run_tweet
            self.__topic = "tweet_topic"
        elif topic == "btc":
            self.run = self.__run_btc
            self.__topic = "btc_topic"
        elif topic == "analysis":
            self.run = self.__run_analysis
            self.__topic = "analysis_topic"
        elif topic == "tps":
            self.run = self.__run_tps
            self.__topic = "tps_topic"
        else:
            return
        # The manager of data is saved here and will be used to save data to mongo
        self.__data_manager = manager
        self.__consumer = KafkaConsumer(
            bootstrap_servers=["127.0.0.1:9092"],
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")))
        self.__consumer.subscribe([self.__topic])

    def __run_btc(self):
        """
        Start to read btc json from topic and send them to data_manager
        """
        for message in self.__consumer:
            self.__data_manager.add_btc(message)

    def __run_tweet(self):
        """
        Start to read tweet json from topic and send them to data_manager
        """
        count = 0
        for message in self.__consumer:
            count = count + 1
            self.__data_manager.add_tweet(message)
            '''if (count % 25000) == 0:
                printpers(print_topic, "TWEET :: Pause")
                time.sleep(100)
                printpers(print_topic, "TWEET :: Start")'''

    def __run_analysis(self):
        """
        Start to read tweet_id from topic and send them to data_manager
        """
        for message in self.__consumer:
            self.__data_manager.save_tweet_cons(json.loads(message.value))

    def __run_tps(self):
        """
        Start to read tps json from topic and send them to data_manager
        """
        for message in self.__consumer:
            self.__data_manager.add_tps(message)