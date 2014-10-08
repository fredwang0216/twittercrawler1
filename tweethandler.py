'''
Created on May 29, 2014

@author: christian
'''
import urllib
import requests
import time
import datetime
import re
import pika
import json
import guess_language
from datetime import datetime
import src
from src.nlp.tokenizer import Tokenizer
from src.search.ngram_search import PlaceFinder
from src.mytwitter.tweetcrawler import FILTER_KEYWORDS
from src.mytwitter.tweet import Tweet
from src.util.http import HttpUtil
from pymongo.mongo_client import MongoClient
from src.mytwitter.locationextractor import LocationExtractor
from src.geo import region

QUEUE_NEW_TWEET_RAW = 'new-tweet-raw'
QUEUE_NEW_TWEET_POS_TAGGED = 'new-tweet-pos-tagged' 

FILTER_OUT_RETWEETS = True

REGION = region.Region()
REGION.init_geo_polygon("/home/christian/work/development/eclipse-workspace/IncidenceReporter/polygon-singapore.txt")
LOCATION_EXTRACTOR = LocationExtractor('/home/christian/data/datasets/english-words/sg-places-names-unique.txt')


class RabbitMqReader:

    def __init__(self, queue_id, callback_method):
        try:
            self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.rabbitmq_channel = self.rabbitmq_connection.channel()
            self.rabbitmq_channel.queue_declare(queue=queue_id)
            
            self.rabbitmq_channel.basic_consume(callback_method, queue=queue_id, no_ack=True)
            self.rabbitmq_channel.start_consuming()
        except:
            print ('Damn!')
            exit(0)

http_util = HttpUtil()

mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['tweety']
mongo_tweets = mongo_db['tweets']

mongo_db_tweetsuite = mongo_client['tweetsuite']
mongo_tweets_ner_loc = mongo_db_tweetsuite['tweetsnerloc']





def print_tweet(channel, method, properties, body):
    print(body)
    
    
    
def store_tweet(channel, method, properties, body):
    try:
        tweet = json.loads(body)
        print(tweet)
        item = { 'tweet' : tweet }
        mongo_tweets.insert(item)
    except ValueError as e:
        print(e)
        

def store_geo_tweet_with_location_candidates(channel, method, properties, body):
    try:
        tweet = json.loads(body)
        #print(tweet['text'])
        
        if FILTER_OUT_RETWEETS:
            if 'retweeted_status' in tweet:
                print 'RETWEET'
        
        coordinates = tweet['coordinates']
        if coordinates is None:
            return
        coordinates = tweet['coordinates']['coordinates']
        if coordinates is None:
            return

        
        lng = tweet['coordinates']['coordinates'][0]
        lat = tweet['coordinates']['coordinates'][1]
        #print lat, lng
        
        if REGION.coord_in_polygon(lat, lng) == False:
            return

        text = tweet['text'].encode('utf-8')
        text = text.replace('\n', '')
        text = text.replace('\r', '')
        location_list = LOCATION_EXTRACTOR.handle_tweet(text)
        
        location_string = ''
        for location in location_list:
            location_string += location[0] + ' => ' + location[1] + '###'
        #location_string = str(location_string)[:-2]
        location_string += ''
        
        if location_string != '':
            print text
            print location_string
            item = { 'text' : text, 'done' : 0 }
            mongo_tweets_ner_loc.insert(tweet)

    except ValueError as e:
        raise e
        
    

        
def publish_tweet(channel, method, properties, body):
    try:
        tweet = json.loads(body)
        print(tweet)
        geo = tweet['geo']
        
        if geo is not None:
            lat = geo['coordinates'][0]
            lng = geo['coordinates'][1]
            name = tweet['user']['name']
            screen_name = tweet['user']['screen_name']
            tweet_id = tweet['id_str']
            user_id = tweet['user']['id']
            created_at = tweet['created_at']
            text = tweet['text']
            text = urllib.quote_plus(text)
            url = u'http://localhost:9010/tweet/new/?tweetid='+str(tweet_id)+'&createdat='+str(created_at)+'&userid='+str(user_id)+'&name='+str(name)+'&screenname='+str(screen_name)+'&lng='+str(lng)+'&lat='+str(lat)+'&text='+str(text)
            print(url)
            r = requests.post(url)
            print(r.text)
        

    except ValueError as e:
        print(e)
        
        
def analyze_tweet(channel, method, properties, body):
    try:
        tweet = Tweet(body)
        place = tweet.getPlaceJson()
        print(tweet.getText())
        print(tweet.getPlaceJson())
        print('')
    except ValueError as e:
        print(e)
        
    
    
    
def handle_tweet_pos_tagged(channel, method, properties, body):

    try:
        body_json = json.loads(body)
    except:
        body_json = body
        
    tweet = body_json['tweet']
    groups = body_json['groups']
        

    #return
        
    #if tweet_json.get('retweeted_status') is not None:
    #    return
#         text = tweet_json['text']
#         text = clean_tweet(text)
#         if 'http' in text:
#             return
#         if len(text) < 100:
#             return
    if not is_ascii(tweet):
        return
    result = guess_language.guessLanguage(tweet)
    if result != 'en':
        return

    new_groups = []
    remove_list = ['singapore']
    for group in groups:
        word_list = group.split()
        new_group = ' '.join([i for i in word_list if i.lower() not in remove_list])
        if new_group != '':
            new_groups.append(new_group)
        
    
        
    print(tweet)
    print(groups)
    print(new_groups)
    
    #tokenizer = Tokenizer(tweet_json['text'])
    #tokens = tokenizer.tokenize_as_tweet()
    #text_snippets = tokenizer.generate_candidate_strings(FILTER_KEYWORDS)
    place_finder = PlaceFinder()
    top_places = place_finder.match_text(new_groups, 1, 3)
    for place in top_places:
        print('TOP PLACE', place_finder.get_place(place[0])['name'], place[1])
    #urls = tokenizer.get_urls()
    #for url in urls:
    #    print(http_util.unshorten_url(url))
    print('')
        
        
def analyze_urls(channel, method, properties, body):
    try:
        tweet = Tweet(body)
        place = tweet.getPlaceJson()
        print(tweet.getText())
        tokenizer = Tokenizer(tweet.getText())
        tokenizer.tokenize_as_tweet();
        urls = tokenizer.get_urls()
        for url in urls:
            print(url)
            unshortened_url = http_util.unshorten_url(url)
            if unshortened_url is not None:
                print(http_util.extract_domain(unshortened_url))
        print('')
    except ValueError as e:
        print(e)
        
    
        
def clean_tweet(text):
    text = text.replace('\r\n', ' ')
    text = text.replace('\n', ' ')
    text = re.sub( '\s+', ' ', text).strip()
    return text

def save_tweet(text):
    dt = datetime.now()
    f = open('tweets.txt', 'a')
    f.write(text.encode('utf8') + '\n') # python will convert \n to os.linesep
    f.close() 

def is_ascii(text):
    try:
        text.decode('ascii')
    except:
        return False
    else:
        return True
    
    
#fetch_tweets_from_db()
if __name__ == "__main__":
    #mongo_tweets.remove()
    rabbit = RabbitMqReader(QUEUE_NEW_TWEET_RAW, store_geo_tweet_with_location_candidates) 
    #rabbit = RabbitMqReader(QUEUE_NEW_TWEET_POS_TAGGED, handle_tweet_pos_tagged)
    #t.__init__()
    #print is_ascii(" dd ")
    
