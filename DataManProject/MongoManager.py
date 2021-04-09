from pymongo import MongoClient
class MongoManager:
    def __init__(self):
        self.__client = MongoClient('127.0.0.1', 27017, username='admin', password='DataMan2019!')
        # 28-41 load server and collections from mongodb, or create and load
        if not ("server" in self.__client.list_database_names()):
            self.__server = self.__client["server"]
        else:
            self.__server = self.__client.server

        if not ("tweet" in self.__server.list_collection_names()):
            self.tweet_collection = self.__client.server["tweet"]
        else:
            self.tweet_collection = self.__client.server.tweet

        if not ("btc" in self.__server.list_collection_names()):
            self.btc_collection = self.__client.server["btc"]
        else:
            self.btc_collection = self.__client.server.btc

        if not ("tps" in self.__server.list_collection_names()):
            self.tps_collection = self.__client.server["tps"]
        else:
            self.tps_collection = self.__client.server.tps

        if not ("sup_tps" in self.__server.list_collection_names()):
            self.tps_sup_collection = self.__client.server["sup_tps"]
        else:
            self.tps_sup_collection = self.__client.server.sup_tps

        if not ("sup_btc" in self.__server.list_collection_names()):
            self.btc_sup_collection = self.__client.server["sup_btc"]
        else:
            self.btc_sup_collection = self.__client.server.sup_btc