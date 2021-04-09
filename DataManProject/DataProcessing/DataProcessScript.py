from DataManager import DataManager
from Consumer import ConsumerUniv
from TweetAnalysis import TweetAnalysis
from Utilities import parallel_create, parallel_run


def main():
    data_manager = DataManager()
    cleaner = TweetAnalysis(data_manager)
    consumer_analysis = ConsumerUniv("analysis", data_manager)
    proc1 = parallel_create(consumer_analysis.run)
    parallel_run(proc1)


if __name__ == '__main__':
    main()




