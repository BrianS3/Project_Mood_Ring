# import sys
# import os
# import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from mysql.connector import connect, Error
import mysql

today = str(datetime.now().replace(microsecond=0))
today2 =str(date.today())
today3 = date.today()
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
current_time = current_time.replace(':', "-")
yesterday = today3-timedelta(days = 1)


token = pd.read_csv('assets/twitterapi.csv')
consumer_key = str(token['APIkey'].values[0])
consumer_secret = str(token['APIseckey'].values[0])

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

host=""
user=""
password=""
database = ""

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")
cursor = cnx.cursor()

query  = "select * from CITIES"
cursor.execute(query)
cities = cursor.fetchall()

for city in cities:
    anchor_city_id = city[0]
    lat = float(city[4])
    lon = float(city[5])

    tweet_lst=[]
    tweet_df = pd.DataFrame(tweet_lst, columns=['TWEET_DT', 'ID', 'ANCHOR_CITY_ID','USERNAME', 'NAME', 'TWEET','GEO'])

    geoc=f"{lat},{lon},25mi" # Not sure what to do about this part...does this need to be changed to anchor city?
    for tweet in tweepy.Cursor(api.search,geocode=geoc).items(1000):
        # if tweet.created_at.date() == yesterday:
        tweetDate = tweet.created_at
        tweet_lst.append([tweetDate,tweet.id,
                tweet.user.screen_name,
                tweet.user.name, tweet.text,
                tweet.user._json['geo_enabled']])


    print(len(tweet_df))

    count_skipped = 0
    count_inserted = 0

    column_list = list(tweet_df.columns)

    dumplist = []
    dumpfile = open(f"reject_log_{anchor_city_id}_{today2}_{current_time}.txt", "w")

    cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',host=f'{host}', database=f"{database}")
    cursor = cnx.cursor()

    for ind, row in tweet_df.iterrows():
        try:
            query = (f"""
            INSERT INTO RAW_TWITTER
            VALUES (
            "{row[column_list[0]]}"
            ,"{row[column_list[1]]}"
            ,"{anchor_city_id}"
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
            dumplist.append(row)
            # print(row)
            continue
    cnx.commit()

    dumpfile.write(str(dumplist))

    cursor.execute(f"""
    INSERT INTO RUN_LOG (SCRIPT, RUN_TIME, ANCHOR_CITY_ID, RECORDS_INSERTED, RECORDS_SKIPPED) 
    VALUES ('twitter_geo_daily', '{today}', '{anchor_city_id}', {count_inserted}, {count_skipped})""")
    cnx.commit()
cnx.close()
