"""
  Convert json lines txt to csv with relevant columns
"""

import os
import argparse
import glob
#
import pandas as pd

def onlyUniques(data_path,out_path):
  """
  convert json to csv
  """
  lst = []
  raw_files = glob.glob(data_path + "/gn_tweet_*.txt")
  raw_files.sort(key=os.path.getmtime)
  #
  file_out = "{}/{}.csv".format(out_path,str(raw_files[-1].split("/")[-1])[:-4])
  try:
    print('Start process...')
    data = pd.concat((pd.read_csv(f,encoding='utf-8') for f in raw_files), ignore_index=True).drop_duplicates(subset='content', keep='first', inplace=False, ignore_index=True).reset_index(drop=True)
    data = data.rename({'id':'tweet_id', 'content':'tweet'}, axis=1)
    data.to_csv(file_out, encoding='utf-8', index=False)
  except Exception as e:
    print('Process aborted', e)
  finally:
    print('...End process')
    return file_out, len(data)


if __name__ == '__main__':
  # cli inputs
  parser = argparse.ArgumentParser()
  parser.add_argument('--experiment', required=True, type=str, help='experiment number ... ?')
  args = parser.parse_args()
  ##############################################################################
  # unpack
  experiment = args.experiment
  #
  
  RAW_TWEET_DIR = 'raw_tweet'+'_'+experiment
  CSV_TWEET_DIR = 'out'+'_'+experiment
  
  # maybe create os dir
  if not os.path.exists(CSV_TWEET_DIR):
    os.makedirs(CSV_TWEET_DIR)
  
  file_csv, count_lines = onlyUniques(RAW_TWEET_DIR,CSV_TWEET_DIR)
  print('CSV file {}, tweets: {}'.format(file_csv, count_lines))

