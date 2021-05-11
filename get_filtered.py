# 0. import
import pandas as pd
import numpy as np
import sys
import glob
from glob import iglob
import dask.dataframe as dd
import re
from utils import datetime_filename, uniqueList, compareTweets
import networkx as nx
import itertools
import random


# 1. args
raw_tweet_dir = sys.argv[1] # data path
sample1000 = sys.argv[2].lower()=='true' # sample of 1000 tweets without lang identification
lookup = sys.argv[3].lower()=='true' # use lookup


# 2. read files
# 2.1 tweets
all_files = glob.glob(raw_tweet_dir + "/gn_*utc.csv")
usecols=['tweet_id','date','tweet','lang_twitter']
raw_tweet_files = dd.read_csv(all_files,usecols=lambda c: c in set(usecols),dtype={'tweet_id': 'str'})
print("raw_tweet_files\n",raw_tweet_files.head(3),"\n",len(raw_tweet_files.tweet_id))
raw_tweet_files = raw_tweet_files.compute()
if sample1000:
    # write tweets
    raw_tweet_files.sample(1000).to_csv(datetime_filename(prefix=raw_tweet_dir+'/gn_tweet_'+str(len(raw_tweet_files.tweet_id))+'_',extension='_sample1000.csv'),encoding='utf-8',index=False)
    print("tweets sample exported...")
    sys.exit(0)
    
# 2.2 lang
all_files = glob.glob(raw_tweet_dir + "/tweets_languages_gn_tweet*utc.csv") # iglob(raw_tweet_dir + "/tweets_languages_gn_tweet*utc.csv", recursive=True)
usecols=['tweet_id', 'lang', 'lang_fasttext', 'lang_polyglot', 'lang_textcat']
raw_lang_files = dd.read_csv(all_files,usecols=lambda c: c in set(usecols),dtype={'tweet_id': 'str'})
#raw_lang_files = pd.concat((pd.read_csv(f,usecols=lambda c: c in set(usecols),dtype={'tweet_id': 'str'}) for f in all_files), ignore_index=True)
print("raw_lang_files\n",raw_lang_files.head(3),"\n",len(raw_lang_files.tweet_id))
raw_lang_files = raw_lang_files.compute()

# 2.3 lookup
if lookup:
  all_files = glob.glob(raw_tweet_dir + "/tweets_languages_gn_gn_tweet*utc.csv")
  usecols=['tweet_id', 'tokens', 'match_contains', 'word_contains', 'match_intersection', 'word_intersection']
  raw_lkp_files = dd.read_csv(all_files,usecols=lambda c: c in set(usecols),dtype={'tweet_id': 'str'})
  print("raw_lkp_files\n",raw_lkp_files.head(3),"\n",len(raw_lkp_files.tweet_id))
  raw_lkp_files = raw_lkp_files.compute()

# 2.4 merge files, drop duplicates
data = pd.merge(raw_tweet_files,raw_lang_files, on='tweet_id', how="left")
if lookup:
  data = pd.merge(data,raw_lkp_files, on='tweet_id', how="left")
data = data.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=True).reset_index(drop=True)
print("merge\n",data.head(3),"\n",len(data.tweet_id))


# 3. get stats
data['date'] = pd.to_datetime(data['date'])

# 3.1 ... and by lang gn
data_gn = data[
( (data['lang_fasttext'].str.contains('gn|gug|grn',regex=True)) | (data['lang_polyglot'].str.contains('gn|gug|grn',regex=True)) | (data['lang_textcat'].str.contains('gn|gug|grn',regex=True)) ) # one return gn
#| ( (data['lang_fasttext'].str.contains('undefined',regex=True)) & (data['lang_polyglot'].str.contains('undefined',regex=True)) & (data['lang_textcat'].str.contains('undefined',regex=True)) ) # all gn support are undefined
]
if lookup:
  data_gn = data_gn[ 
  ( (data_gn['match_intersection']>=1) & ((data_gn['match_contains']/data_gn['tokens'])>=0.5) ) # exact match >= 1 and partial match >= 50%
  ]

# 3.2 exclude words: top frequency, not guarani
if lookup:
  exclude = ['are','pota','nicaragua','mano'] 
  data_gn = data_gn[(~data_gn.tweet.str.lower().str.contains("|".join(exclude),regex=True))]

# 3.3 try to drop duplicates
data_gn = data_gn.drop_duplicates(subset='tweet_id', keep='first', inplace=False, ignore_index=True).reset_index(drop=True)
data_gn = data_gn.drop_duplicates(subset='tweet', keep='first', inplace=False, ignore_index=True).reset_index(drop=True)
print("data_gn\n",data_gn.sample(3),"\n",len(data_gn.tweet_id))
#del data

# 3.4 remove very similar
# create graph objects
nodes = data_gn.index.tolist()
edges = [*itertools.chain(*data_gn.sort_values(by="tweet", key=lambda x: x.str.len()).tweet.apply(lambda a: compareTweets(data_gn,'tweet',a,0.60)))] #
# create graphs
G = nx.Graph()
G.add_nodes_from(nodes)
G.add_edges_from(edges)
# get connected component indexes
grouped_indexes = [*nx.connected_components(G)]
# get a random choice index from each group
#filtered_indexes = [random.choice([*_]) for _ in grouped_indexes]
# get the first, which the length is minor (edges)
filtered_indexes = [list(_)[0] for _ in grouped_indexes]
data_gn = data_gn.loc[filtered_indexes].reset_index(drop=True)
print("uniques\n",data_gn.sample(3),"\n",len(data_gn.tweet_id))


# 4. export data
# 4.1 write tweets
data_gn.to_csv(datetime_filename(prefix=raw_tweet_dir+'/gn_tweet_',extension='_filtered.csv'),index=False)
print("tweets exported...")
#del data_gn

# 4.2 merge with existing filtered files
# 4.2.1 load tweets
all_files = iglob(raw_tweet_dir + "/gn_*utc_filtered.csv", recursive=True)
usecols=['tweet_id','tweet','lang_human','date','lang_twitter','lang_fasttext',
'lang_polyglot','lang_textcat','tokens','match_contains','word_contains','match_intersection','word_intersection']
gn_tweet_files = pd.concat((pd.read_csv(f,usecols=lambda c: c in set(usecols),dtype={'tweet_id': 'str'}) for f in all_files), ignore_index=True)
# 4.2.2 group by tweets
gn_tweet_files = gn_tweet_files.fillna('')
if 'lang_human' not in gn_tweet_files:
  gn_tweet_files['lang_human'] = ''
if 'lang_twitter' not in gn_tweet_files:
  gn_tweet_files['lang_twitter'] = '' 
if 'lang_twitter' not in data_gn:
  data_gn['lang_twitter'] = ''
if lookup:
  attr = {
                                 'tweet':'first',  
                                 'date':'first', 
                                 'lang_human': ','.join,
                                 'lang_twitter':'first', 
                                 'lang_fasttext':'first', 
                                 'lang_polyglot':'first', 
                                 'lang_textcat':'first', 
                                 'tokens':'first', 
                                 'match_contains':'first', 
                                 'word_contains':'first', 
                                 'match_intersection':'first', 
                                 'word_intersection':'first',
                                 }
else:
   attr = {
                                 'tweet':'first',  
                                 'date':'first', 
                                 'lang_human': ','.join,
                                 'lang_twitter':'first', 
                                 'lang_fasttext':'first', 
                                 'lang_polyglot':'first', 
                                 'lang_textcat':'first', 
                                 } 
gn_tweet_files = gn_tweet_files.groupby(['tweet_id']).agg(attr).reset_index()
gn_tweet_files['lang_human'] = gn_tweet_files['lang_human'].apply(lambda a: ','.join(uniqueList(a.split(',')))) # Drop duplicate terms in column
print("gn_tweet_files\n",gn_tweet_files.head(3),"\n",len(gn_tweet_files.tweet_id))
# 4.2.3 rewrite tweets
gn_tweet_files.to_csv(datetime_filename(prefix=raw_tweet_dir+'/gn_tweet_',extension='_filtered.csv'),encoding='utf-8',index=False)
print("tweets re-exported...")

# 4.3 write stats
dataC = data_gn.groupby([data_gn.date.dt.year, data_gn.date.dt.month, data_gn.lang, data_gn.lang_fasttext, data_gn.lang_polyglot, data_gn.lang_textcat, data_gn.lang_twitter]).count()
with open(datetime_filename(prefix=raw_tweet_dir+'/gn_statistics_date_',extension='.tsv'),'w') as write_tsv:
    write_tsv.write(dataC[['date']].to_csv(sep='\t', encoding='utf-8'))
#del dataC
print("stats generated...")

