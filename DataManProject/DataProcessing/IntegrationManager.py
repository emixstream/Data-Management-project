from DataProcessing.DataManager import DataManager
from Utilities import parallel_run, parallel_create, printpers
import time
from colorama import Fore, Style

print_topic = Fore.GREEN + "IntegrationManager" + Style.RESET_ALL
print_at = 1


class IntegrationManager:
    __general_count_processed = 0
    __general_count_processed_single = 0
    __process_ready_tweet = list()  # List of tweet Ids ready to be analyzed
    __new_id_integrate = False  # True if __process_ready_tweet is not empty
    __time_s = 0

    def __init__(self, data_manager=DataManager()):
        """
        This class will associate each sentiment mean and transaction per second to the btc values
        :param data_manager: DataManager
        """
        self.__data_manager = data_manager
        self.__t_integrate = parallel_create(self.__run_integration)

    def add_id(self, ts_id):
        """
        Append a ts_id to the list of id to integrate and strart the thread if it is stopped
        :param ts_id: id
        :return: none
        """
        self.__process_ready_tweet.append(ts_id)
        if not self.__new_id_integrate:
            self.__new_id_integrate = True
            parallel_run(self.__t_integrate)

    def __run_integration(self):
        """
        Runs integration
        :return:
        """
        if self.__time_s == 0: self.__time_s = time.time()
        time_zero = time.time()
        last_processed = 0  # last ts_id integrated
        while self.__new_id_integrate:  # run while true
            key = self.__process_ready_tweet.pop(0)  # get key
            tps_value = self.__data_manager.get_tps_ts_id(key)  # get tps value from mongo
            if (self.__data_manager.get_btc_ts_id(key) is None) or (
                    tps_value is None):  # check if btc for this key is on mongo otherwise append key as last to be processed
                # if I had processed(last_processed) a ts_id which is at least 2 minutes grater than the actually ts_id-
                # -I can assume mongo will never receive the record and I can discard this ts_id
                if (last_processed - int(key)) > 1:
                    if len(self.__process_ready_tweet) == 0:
                        self.__t_integrate = parallel_create(self.__run_integration)
                        self.__new_id_integrate = False
                    continue
                self.__process_ready_tweet.append(key)
                continue
            self.__data_manager.integrate_sent_tps(key, tps_value['value'] if not tps_value is None else '')  # integrate value on db
            last_processed = int(key)

            # Timing don't care
            self.__general_count_processed = self.__general_count_processed + 1
            if self.__general_count_processed % print_at == 0:
                time_int = time.time() - self.__time_s
                time_int_zero = time.time() - time_zero
                time_zero = time.time()
                printpers(print_topic, "Integrated : " + str(self.__general_count_processed) + " :: Available : " + str(
                    len(self.__process_ready_tweet)) + " :: TimeTot : " + str(time_int) + " :: Time : " + str(
                    time_int_zero))

            if len(self.__process_ready_tweet) == 0:
                self.__t_integrate = parallel_create(self.__run_integration)
                self.__new_id_integrate = False
