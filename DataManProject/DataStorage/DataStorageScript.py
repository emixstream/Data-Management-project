from Consumer import ConsumerUniv
from DataManager import DataManager
from threading import Thread


# remember to install pip install dnspython

def main():
    data_manager = DataManager()
    data_manager.start_int_producer()
    consumer_tweet = ConsumerUniv("tweet", data_manager)
    consumer_btc = ConsumerUniv("btc", data_manager)
    consumer_tps = ConsumerUniv("tps", data_manager)
    proc1 = Thread(target=consumer_tweet.run)
    proc1.start()
    proc2 = Thread(target=consumer_btc.run)
    proc2.start()
    proc3 = Thread(target=consumer_tps.run)
    proc3.start()
    print("ready")


if __name__ == '__main__':
    main()
