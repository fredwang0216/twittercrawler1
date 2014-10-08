'''
Created on May 29, 2014

@author: christian
'''
import requests
import pika
import json
from src.geo import region
from src.mytwitter.locationextractor import LocationExtractor

QUEUE_NEW_TWEET_RAW = 'new-tweet-raw'
FILTER_OUT_RETWEETS = True

REGION = region.Region()
REGION.init_geo_polygon("/home/christian/work/development/eclipse-workspace/IncidenceReporter/polygon-singapore.txt")
LOCATION_EXTRACTOR = LocationExtractor('/home/christian/data/datasets/english-words/sg-places-names-unique.txt')


class GeoTweetPublisher:

    def __init__(self, queue_id, callback_method):
        try:
            
            self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.rabbitmq_channel = self.rabbitmq_connection.channel()
            self.rabbitmq_channel.queue_declare(queue=queue_id)
            
            self.rabbitmq_channel.basic_consume(callback_method, queue=queue_id, no_ack=True)
            self.rabbitmq_channel.start_consuming()
        except ValueError as e:
            print e
            

        
        
def publish_tweet(channel, method, properties, body):
    try:
        tweet = json.loads(body)
        print(tweet['text'])
        
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
        location_list = LOCATION_EXTRACTOR.handle_tweet(text)
        
        location_string = ''
        for location in location_list:
            location_string += location[0] + ' => ' + location[1] + '###'
        #location_string = str(location_string)[:-2]
        location_string += ''
        
        if location_string != '':
            #print '>>>', '[' + location_string + ']'
            tweet['locations'] = location_string
        
        json_data = json.dumps(tweet)
        post_data = json_data.encode('utf-8')

        headers = {}
        headers['Content-Type'] = 'application/json'

        r = requests.post('http://localhost:9010/tweet/new/', data=post_data, headers=headers)

    except ValueError as e:
        raise e
        
    
if __name__ == "__main__":
    #while(True):
        #try:
    rabbit = GeoTweetPublisher(QUEUE_NEW_TWEET_RAW, publish_tweet)
        #except:
        #3    pass 
    
