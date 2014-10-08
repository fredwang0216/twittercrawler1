'''
Created on Mar 15, 2014

@author: vdw
'''
from tweepy.error import TweepError
import tweepy
import pika





CONSUMER_KEY = 'XBv5hOeVFsC0gYWBqGWTlpNe7' 
CONSUMER_SECRET = 'fjMzRpy5YlGlEIRI7f3ETCNHuTq5n0SeJy0KWJcpSWKQvZjXzT' 
ACCESS_TOKEN_KEY = '2242680794-Nrd574NA2l9FF77dm43Tb3OfEL6ogb3dvzSh2o4' 
ACCESS_TOKEN_SECRET = 'b6BowpiWOxuB0IpzpkGgl5y0FPeFXLAYr4o6kf187WCBF'

QUEUE_NEW_TWEET_RAW = 'new-tweet-raw'

FILTER_KEYWORDS = ['singapore']
FILTER_LANGUAGES = ['en']
FILTER_LOCATIONS_ALL=[-180,-90,180,90]
FILTER_LOCATIONS_SINGAPORE=[103.600333,1.199395,104.087852,1.476724]

class TwitterStream:



    def __init__(self):
        try:
            self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.rabbitmq_channel = self.rabbitmq_connection.channel()
            self.rabbitmq_channel.queue_declare(queue=QUEUE_NEW_TWEET_RAW)

            self.auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
            self.auth.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
            self.api = tweepy.API(self.auth)

            self.sapi = tweepy.streaming.Stream(self.auth, CustomStreamListener(self.api, self.rabbitmq_channel))
            #self.sapi.filter(track=FILTER_KEYWORDS, languages=FILTER_LANGUAGES)
            self.sapi.filter(locations=FILTER_LOCATIONS_SINGAPORE, languages=FILTER_LANGUAGES)
        except TweepError as e: 
            print("Unable to authenticate", e)



    def get_user_info(self, user_name):
        user = self.api.get_user(user_name)
        print(user)


class CustomStreamListener(tweepy.StreamListener):
    
    def __init__(self, api, rabbitmq_channel):
        self.api = api
        self.rabbitmq_channel = rabbitmq_channel
        super(tweepy.StreamListener, self).__init__()

    def on_data(self, tweet):
        #print tweet
        self.rabbitmq_channel.basic_publish(exchange='', routing_key=QUEUE_NEW_TWEET_RAW, body=tweet)
        return True

    def on_error(self, status_code):
        print("[error] " + str(status_code))
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream


if __name__ == "__main__":
    
    t = TwitterStream()

