"""
  Get live/old tweets and save to disk
"""

import os
import json
from twitter import Api
from goto import with_goto
from utils import datetime_filename, get_track_words, set_words, set_grams
import time
#
import sys
import snscrape.modules.twitter as sntwitter
import csv
import inspect


# params
experiment = sys.argv[1]
byApi = str(sys.argv[2]).lower()=='true'
#track_file = sys.argv[2]


RAW_TWEET_DIR = 'raw_tweet'+'_'+experiment
# maybe create raw_tweet dir
if not os.path.exists(RAW_TWEET_DIR):
  os.makedirs(RAW_TWEET_DIR)
  

# retrieve credentials
with open('credentials.json') as j:
  cred = json.load(j)

api = Api(cred['CONSUMER_KEY'], cred['CONSUMER_SECRET'],
          cred['ACCESS_TOKEN'], cred['ACCESS_TOKEN_SECRET'],
          tweet_mode='extended')

track_lst_1 = list(set_words('aux/archivoe-g.csv'))
track_lst_2 = list(set_grams('aux/grams',100))
track_lst_3 = ["reikuaaveta"] # track sns history: remiandu, ndishpy(*2), chereraugo(x), Pontifex_grn, lenguaguarani, enga_paraguayo, SPL_Paraguay, guaranime, avañe'eme, avañe'ēme, avañe'ëme, avañe'ême, guaraníme, rubencarlosoje1, #marandu, reikuaavéta, hesegua, reheguápe, rejuhúta, reikuaaveta, reheguape, rejuhuta


@with_goto
def scrape(tweets_per_file=100000, words_per_track=50):
  """
  for easier reference, we save 100k tweets per file
  """
  f = open(datetime_filename(prefix=RAW_TWEET_DIR+'/gn_tweet_'), 'w')
  tweet_count = 0
  hour_count = 0
  #track_grams #14 # track_grams #0
  #track_words #73 #31 #18 #0
  try:
    label .next_
    start_time = time.time()
    tracking = get_track_words(words_per_track,hour_count,track_lst_2) # change here the track list
    print(tracking, len(tracking))
    for line in api.GetStreamFilter(follow=None, track=tracking, locations=None, languages=None, delimited=None, stall_warnings=None, filter_level=None):
      if 'text' in line: #and line['lang'] == u'und':
        #text = line['text'].encode('utf-8').replace('\n', ' ')
        f.write('{}\n'.format(line))
        tweet_count += 1
        if tweet_count % tweets_per_file == 0: # start new batch
          f.close()
          f = open(datetime_filename(prefix=RAW_TWEET_DIR+'/gn_tweet_'), 'w')
          continue
      next_time = time.time()
      print(next_time, start_time, next_time - start_time)
      if int((next_time - start_time)/3600.0)>=3: # trunc 3h track1, track2; 
        hour_count += 1
        goto .next_
  except KeyboardInterrupt:
    print('Twitter stream collection aborted')
  finally:
    f.close()
    return tweet_count


# web scraping with snscrape
def get_old_tweets(check_keys=True):
  tweet_count = 0
  csvFile = open(datetime_filename(prefix=RAW_TWEET_DIR+'/gn_tweet_'), 'a')
  csvWriter = csv.writer(csvFile)

  try:
    maxTweets = -1  # the number of tweets you require
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(" ".join(track_lst_3)+' since:2006-03-21 until:2021-01-31').get_items()): # " "=and
      if i >= maxTweets and maxTweets > -1:
        break
      print(i,[t for t in (tweet.__class__.__dict__['_fields'])],"=",[t for t in tweet])
      #
      if tweet_count==0: 
        csvWriter.writerow([t for t in tweet.__class__.__dict__['_fields']]) # write header
      if check_keys:
        text =str(tweet.content+tweet.username).lower()
        if any(word.lower() in text for word in track_lst_3): # strict check if have any keywords (strict: e.g., key=día, text|user=dia, then exclude it )
          csvWriter.writerow([attr for attr in tweet]) # I want all attr, If you need less information, just provide the attributes
      else:
        csvWriter.writerow([attr for attr in tweet]) # I want all attr, If you need less information, just provide the attributes
      tweet_count += 1
  except KeyboardInterrupt:
    print('Twitter scrape collection aborted')
  finally:
    csvFile.close()
    return tweet_count
        

if __name__ == '__main__':
  if byApi:
    tweet_count = scrape()
  else:
    tweet_count = get_old_tweets()
  print('A total of {} tweets collected'.format(tweet_count))

