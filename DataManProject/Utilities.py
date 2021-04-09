from threading import Thread
from MongoManager import MongoManager

mongo_manager = MongoManager()


def parallel_run(th):
    th.start()


def parallel_create(owner):
    return Thread(target=owner)


def parallel_create_w_args(owner, args=None):
    return Thread(target=owner, args=(args,))


def printpers(topic, text):
    print("CLI ", topic, ":: ", text, "\n")


is_realtime = True  # Set True to run RealTime Version; False otherwise

analysis_thread = 2  # MAX 4

# Set for RealTime Version
chrome_binary_location = "/Users/valerioschips/Desktop/DataManagement/Project/DataManProject/RealTimeProducer/Google " \
                         "Chrome.app/Contents/MacOS/Google Chrome"  # Set Browser directory for scrapers;
chromedriver_path = "/Users/valerioschips/Desktop/DataManagement/Project/" \
                    "DataManProject/RealTimeProducer/chromedriver"  # Set chromedriver directory for scrapers;

# Set for Non Realtime Version
path_to_btc_csv = "BitCoin.csv"
path_to_tweet_csv = "Tweet.csv"
path_to_tps_csv = "TPS.csv"
