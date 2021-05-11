"""
utils
"""
import pandas as pd
import json
import ast
from operator import itemgetter        
import csv
import datetime
import glob
import string
#
import Levenshtein
import preprocessor as p


p.set_options(p.OPT.URL, p.OPT.MENTION, p.OPT.RESERVED) # remove this options


def datetime_filename(prefix='output_',extension='.txt'):
  """
  creates filename with current datetime string suffix
  """
  outputname = prefix + '{:%Y%m%d%H%M%S}utc{}'.format(
    datetime.datetime.utcnow(),extension)
  return outputname
  

def get_track_words(words_per_track,hour_count,lst):
  """
  read a list with words in gn
  """
  i = hour_count * words_per_track 
  j = i + words_per_track - 1  
  
  return lst[i:j]


def set_words(data_path):
  """
  read a csv dictionary with words in gn
  """
  w_df = pd.read_csv(data_path, names=['es','gn','syn1','syn2'], encoding='iso-8859-1') # file -i
  gn_df = w_df[['gn','syn1','syn2']].drop_duplicates()
  gn_lst = gn_df['gn'].tolist()+gn_df['syn1'].tolist()+gn_df['syn2'].tolist()
  cleanedList = [x for x in gn_lst if str(x) != 'nan' and len(x)>=3]
  gn_set = set(cleanedList)
  
  print(len(gn_set))
  
  f = open(data_path[:-4]+".txt", 'w')
  for w in gn_set:
    f.write('{}\n'.format(w))
  f.close()
  
  return list(gn_set)

  
def set_grams(data_path,top=100):
  """
  read a dict with 'terms frequency' in gn
  """
  files = glob.glob(data_path + "/*/*word*.txt") # txt files in subfolders
  ngram = []
  table = str.maketrans("","",string.punctuation)
  for f_in in files:
    with open(f_in, 'r') as fi:
      for lines in fi:
        item = lines.replace("\n","").split()
        term = ""
        count = 0
        if len(item)==3: # bigrams
          term0 = str(item[0]).translate(table).strip()
          term1 = str(item[1]).translate(table).strip()
          term = "{},{}".format(term0,term1) if (len(term0)>2 and len(term1)>2 and not term0.isnumeric() and not term1.isnumeric()) else (term0 if (len(term0)>2 and not term0.isnumeric()) else (term1 if (len(term1)>2 and not term1.isnumeric()) else ""))  # comma(,) for OR in Twitter 
          count = int(item[2])
        elif len(item)==2: # unigrams
          term = str(item[0]).translate(table).strip()
          count = int(item[1])
        if count>=top and str(term) != 'nan' and len(term)>=3: # ignore term freq minor than top and term length than 3
          ngram.append(term)
    fi.close()
  gn_set = set(ngram)
  
  print(len(gn_set))
  
  f = open(data_path+".txt", 'w')
  for w in gn_set:
    f.write('{}\n'.format(w))
  f.close()
  
  return list(gn_set)
  
  
def read_tweets(data_path):
    """
    read a file with tweets in json and convert to csv
    """

    json_list = []
    with open(data_path, 'r') as json_file_:
      for line in json_file_:
        json_file = json.dumps(ast.literal_eval(line))
        json_list += json_file,
    
    header = ['tweet_id', 'tweet', 'date', 'lang_twitter', 'retweeted', 'user_id']
    required_cols = itemgetter(*header)

    #with open(data_path) as f_input, open('out/'+data_path[:-4]+'.csv', 'w', newline='') as f_output:
    output = data_path.split("/")[-1]
    output = 'out/{}.csv'.format(output[:-4])
    with open(output, 'w', newline='') as f_output:
        csv_output = csv.writer(f_output)
        csv_output.writerow(header)
        for row in json_list:
            if row.strip():
                tweet = json.loads(row)
                tweet['tweet_id'] = tweet['id_str']
                tweet['tweet'] = tweet['extended_tweet']['full_text'] if ("extended_tweet" in tweet or "full_text" in tweet) and bool(tweet["truncated"]) else tweet['text']
                tweet['date'] = tweet['created_at']
                tweet['lang_twitter'] = tweet['lang']
                tweet['user_id'] = tweet['user']['id_str']
                csv_output.writerow(required_cols(tweet))
                
    return True


## uniques
def uniqueList(l):
    ulist = []
    [ulist.append(x.strip().lower()) for x in l if x.strip().lower() not in ulist and x.strip().lower() not in ['nan','na','',None]]
    return ulist  


def compareTweets(df, col, tweet1, threshold=0.90):
    tweet1_clean = p.clean(tweet1)
    # compare tweets using Levenshtein distance (or whatever string comparison metric) 
    matches = df[col].apply(lambda tweet2: (Levenshtein.ratio(tweet1_clean, p.clean(tweet2)) >= threshold))

    # get positive matches
    matches = matches[matches].index.tolist()

    # convert to list of tuples
    return [*zip(iter(matches[:-1]), iter(matches[1:]))]
      
