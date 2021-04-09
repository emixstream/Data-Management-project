from tweepy.streaming import StreamListener
import tweepy
from tweepy import Stream
from kafka import KafkaProducer
import json

access_token = '380441074-m0Zt2KceLqPsSo7iFMWvMP0bmNTPp6YllocBmEXY'
access_token_secret = 'MDrhEdQj4EvkzlA8jFpigNEtxNMG9COuYFD5gBbTRSvGW'
consumer_key = 'tUVcQVTs4Vme2sOvYQrmroElq'
consumer_secret = 'Fce6Ygz0ZgdD88AXZmAUuD1aZpsATwhNS18GjzkHx3yKRe1Kna'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

topic = "tweet_topic"


class TweetListener(StreamListener):
    __sent_count = 0

    def __init__(self, producer, api):
        self.__producer = producer
        self.api = api

    def on_status(self, status):
        tweet_elem = {"tweet_id": status.id, "user_screen_name": status.user.screen_name,
                      "tweet_text": str(status.text).replace(";", ",,"),
                      "time": status.created_at.strftime("%Y-%m-%d %H:%M:%S"), "tweet_geo": status.geo,
                      "tweet_retweet": status.retweet_count, "tweet_favourite": status.favorite_count,
                      "tweet_device": str(status.source).replace(";", ",,"),
                      "tweet_hashtag": [x['text'] for x in status.entities["hashtags"]],
                      "user_description": str(status.user.description).replace(";", ",,"),
                      "user_follower": status.user.followers_count, "user_verified": status.user.verified}
        self.__producer.send(topic=topic, value=json.dumps(tweet_elem))
        print(tweet_elem)
        self.__sent_count = self.__sent_count + 1


def start_scrape_tweet(producer=KafkaProducer(bootstrap_servers=["127.0.0.1:9092"],
                                              value_serializer=lambda v: json.dumps(v).encode("utf-8"))):
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    stream = Stream(auth=api.auth, listener=TweetListener(producer, api))
    stream.filter(track=["BTC", "Bitcoin", "bitcoin"], languages=["en"])
    return stream


if __name__ == '__main__':
    start_scrape_tweet()
