'''
Created on Mar 15, 2014

@author: vdw
'''
import twython
import tweepy
import json

CONSUMER_KEY = 'XBv5hOeVFsC0gYWBqGWTlpNe7' 
CONSUMER_SECRET = 'fjMzRpy5YlGlEIRI7f3ETCNHuTq5n0SeJy0KWJcpSWKQvZjXzT' 
ACCESS_TOKEN_KEY = '2242680794-Nrd574NA2l9FF77dm43Tb3OfEL6ogb3dvzSh2o4' 
ACCESS_TOKEN_SECRET = 'b6BowpiWOxuB0IpzpkGgl5y0FPeFXLAYr4o6kf187WCBF'


FILTER_KEYWORDS = ['singapore']
FILTER_LANGUAGES = ['en']

class SimpleTweetStreamer:



    def __init__(self):
        try:
            # tweepy stuff
            #self.auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
            #self.auth.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
            #self.api = tweepy.API(self.auth)

            #self.sapi = tweepy.streaming.Stream(self.auth, MyTweepyStreamListener(self.api))
            #self.sapi.filter(track=FILTER_KEYWORDS, languages=FILTER_LANGUAGES)
            
            
            # twython stuff
            self.twython_stream = MyTwythonStreamerListener(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
            self.twython_stream.statuses.filter(track=FILTER_KEYWORDS, languages=FILTER_LANGUAGES)
            
        except Exception as e: 
            print e


# 
# class MyTweepyStreamListener(tweepy.StreamListener):
#     
#     def __init__(self, api):
#         self.api = api
#         super(tweepy.StreamListener, self).__init__()
# 
#     def on_data(self, body):
#         try:
#             tweet = json.loads(body)
#             print tweet['created_at'], tweet['text']
#         except:
#             pass
#         return True
# 
#     def on_error(self, status_code):
#         print("[error] " + str(status_code))
#         return True # Don't kill the stream
# 
#     def on_timeout(self):
#         return True # Don't kill the stream




class MyTwythonStreamerListener(twython.TwythonStreamer):
    
    def on_success(self, data):
        if 'text' in data:
            print data['text'].encode('utf-8')
        # Want to disconnect after the first result?
        # self.disconnect()
    
    def on_error(self, status_code, data):
        print status_code, data




if __name__ == "__main__":
    
    t = SimpleTweetStreamer()

