import sys
import os
import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from mysql.connector import connect, Error
import mysql

today = str(datetime.now().replace(microsecond=0))

token = pd.read_csv('assets/twitterapi.csv')
consumer_key = str(token['APIkey'].values[0])
consumer_secret = str(token['APIseckey'].values[0])

# Authenticate
auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
# if (not api):
#     print (“Can’t Authenticate”)
#     sys.exit(-1)

tweet_lst=[]
geoc="38.9072,-77.0369,1mi"
for tweet in tweepy.Cursor(api.search,geocode=geoc).items(1000):
    tweetDate = tweet.created_at.date()
    # print(tweet.created_at.date())
    if(tweet.coordinates !=None):
        tweet_lst.append([tweetDate,tweet.id,tweet.
                coordinates['coordinates'][0],
                tweet.coordinates['coordinates'][1],
                tweet.user.screen_name,
                tweet.user.name, tweet.text,
                tweet.user._json['geo_enabled']])

tweet_df = pd.DataFrame(tweet_lst, columns=['TWEET_DT', 'ID', 'LAT','LONGITUDE','USERNAME', 'NAME', 'TWEET','GEO'])

print(tweet_df)

host=""
user=""
password=""
database = ""

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")
cursor = cnx.cursor()

count_skipped = 0
count_inserted = 0

column_list = list(tweet_df.columns)

for ind, row in tweet_df.iterrows():
    try:
        query = (f"""
        INSERT INTO TWITTER_API_TEST_2
        VALUES (
        "{row[column_list[0]]}"
        ,"{row[column_list[1]]}"
        ,"{row[column_list[2]]}"
        ,"{row[column_list[3]]}"
        ,"{row[column_list[4]]}"
        ,"{row[column_list[5]]}"
        ,"{row[column_list[6]]}"
        ,"{row[column_list[7]]}"
        );
        """)
        cursor.execute(query)
        count_inserted+=1
    except: 
        count_skipped+=1
        # print(row)
        continue
cnx.commit()

cursor.execute(f"""
INSERT INTO RUN_LOG (SCRIPT, RUN_TIME, RECORDS_INSERTED, RECORDS_SKIPPED) 
VALUES ('TWITTER_API_TEST_2', '{today}', {count_inserted}, {count_skipped})""")
cnx.commit()
cnx.close()