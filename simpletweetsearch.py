'''
Created on Jul 20, 2014

@author: christian
'''
import tweepy
import twython

CONSUMER_KEY = 'XBv5hOeVFsC0gYWBqGWTlpNe7' 
CONSUMER_SECRET = 'fjMzRpy5YlGlEIRI7f3ETCNHuTq5n0SeJy0KWJcpSWKQvZjXzT' 
ACCESS_TOKEN_KEY = '2242680794-Nrd574NA2l9FF77dm43Tb3OfEL6ogb3dvzSh2o4' 
ACCESS_TOKEN_SECRET = 'b6BowpiWOxuB0IpzpkGgl5y0FPeFXLAYr4o6kf187WCBF'


class SimpleTweetSearch:
    
    def __init__(self):
        self.results_per_page = 10

        self.auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        self.auth.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
        self.api = tweepy.API(self.auth)

        self.twitter = twython.Twython(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)


    def search_tweepy(self, search_string):
        result = tweepy.Cursor(self.api.search, q=search_string, count=self.results_per_page, result_type="recent", include_entities=True, lang="en")
        print result
        for tweet in result.items():
            print tweet.created_at, tweet.text
            #print tweet


    def search_twython(self, search_string):
        try:
            search_results = self.twitter.search(q=search_string, result_type="recent", include_entities=True, lang="en", count=self.results_per_page)
        except twython.TwythonError as e:
            print e
            
        for tweet in search_results['statuses']:
            print tweet['created_at'], tweet['text']

    
if __name__ == "__main__":
    
    sts = SimpleTweetSearch()
    
    sts.search_twython('singapore')
    
    