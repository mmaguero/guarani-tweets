"""
  Convert json lines txt to csv with relevant columns
"""

import os
import argparse
import glob
from utils import read_tweets


RAW_TWEET_DIR = 'raw_tweet'
CSV_TWEET_DIR = 'out'

# maybe create os dir
if not os.path.exists(CSV_TWEET_DIR):
  os.makedirs(CSV_TWEET_DIR)


def tocsv(lang_detection,include_current,data_path=RAW_TWEET_DIR,out_path=CSV_TWEET_DIR):
  """
  convert json to csv
  """
  lst = []
  raw_files = glob.glob(data_path + "/gn_tweet_*.txt")
  raw_files.sort(key=os.path.getmtime)
  #
  csv_files = [name[:-4].replace(out_path + "/","") for name in glob.glob(out_path + "/gn_tweet_*utc.csv")]
  print(csv_files)
  # include current scrape 
  raw_files = raw_files if include_current else raw_files[:-1]
  try:
    print('Start process...')
    for filename in raw_files: 
      # get file name
      json_vs_csv = filename.split("/")
      name = json_vs_csv[-1].split(".")[0] # i just want the file name without extension
      if name not in csv_files: # if csv do not exists
        # to csv
        print(name)
        if read_tweets(filename):
          lst.append(name)
      else:
        print(name, "has been already processed")
  except Exception as e:
    print('Process aborted', e)
  finally:
    print('...End process')
    return lst


if __name__ == '__main__':
  # cli inputs

  parser = argparse.ArgumentParser()
  parser.add_argument('--lang_detection', required=False, type=bool, default=False,
                      help='detect lang of tweets?')
  parser.add_argument('--include_current', required=False, type=bool, default=False,
                      help='include current scrape?')
  args = parser.parse_args()
  ##############################################################################
  # unpack
  lang_detection = args.lang_detection # to-do
  include_current = args.include_current
  #
  
  tweet_csv = ", ".join(tocsv(lang_detection,include_current))
  print('CSV tweets {}'.format(tweet_csv))

