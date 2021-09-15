from mysql.connector import connect, Error
import mysql
from graphipy.graphipy import GraphiPy
import networkx as nx
import pandas as pd
from datetime import datetime as dt
import pandas as pd

today = str(dt.now().replace(microsecond=0))

graphipy = GraphiPy()
token = pd.read_csv('assets/twitterapi.csv')

CONSUMER_KEY = str(token['APIkey'].values[0])
CONSUMER_SECRET = str(token['APIseckey'].values[0])
ACCESS_TOKEN = ""
TOKEN_SECRET = ""
twitter_api_credentials = {
    "consumer_key": CONSUMER_KEY,
    "consumer_secret": CONSUMER_SECRET,
    "access_token": ACCESS_TOKEN,
    "token_secret": TOKEN_SECRET
}

twitter = graphipy.get_twitter(twitter_api_credentials)

keyword = "Machine Learning"
limit = 5000
tweets_graph = twitter.fetch_tweets_by_topic(graphipy.create_graph(), keyword, limit)

tweets_df = tweets_graph.get_df("tweet")
tweets_df.drop(columns='source', inplace=True)
column_list = list(tweets_df.columns)

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")
cursor = cnx.cursor()

count_skipped = 0
count_inserted = 0

for ind, row in tweets_df.iterrows():
    try:
        query = (f"""
        INSERT INTO TWITTER_API_TEST_1
        VALUES (
        "{row[column_list[0]]}"
        ,"{row[column_list[1]]}"
        ,"{row[column_list[2]]}"
        ,"{row[column_list[3]]}"
        ,"{row[column_list[4]]}"
        ,"{row[column_list[5]]}"
        ,"{row[column_list[6]]}"
        ,"{row[column_list[7]]}"
        ,"{row[column_list[8]]}"
        ,"{row[column_list[9]]}"
        ,"{row[column_list[10]]}"
        ,"{row[column_list[11]]}"
        ,"{row[column_list[12]]}"
        ,"{row[column_list[13]]}"
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
VALUES ('TWITTER_API_TEST_1', '{today}', {count_inserted}, {count_skipped})""")
cnx.commit()
cnx.close()

