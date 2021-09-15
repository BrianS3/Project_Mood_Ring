import pandas as pd
import mysql
from mysql.connector import connect, Error
import re

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")

cursor = cnx.cursor()

tweets = pd.read_sql_query(""" 
select * from VW_TWEET_TEXT
""", cnx)

words_lex = pd.read_csv('../csv_files/emotion_lexicon_word_level.csv')
words_lex = words_lex[words_lex['association']==1]
words_lex.reset_index(inplace=True)
words_lex.drop('index', axis=1, inplace=True)

association = pd.DataFrame(columns=['tweet_id', 'term', 'emotion'])

tweets['TWEET_TEXT_CLEAN'] = tweets['TWEET_TEXT'].apply(lambda x: re.sub(r'https?:\/\/.*', '', x))
tweets['TWEET_TEXT_CLEAN'] = tweets['TWEET_TEXT'].apply(lambda x: re.sub(r"['@'][A-z0-9]+", '', x))
tweet_dict = dict(zip(tweets['TWEET_ID'], tweets['TWEET_TEXT_CLEAN']))


for key, val in tweet_dict.items():
    for word in val.split():    
        for item in words_lex.values.tolist():
            if item[0] == word:
                association.loc[len(association.index)] = [key, item[0], item[1]]


association.rename(columns={'tweet_id':'TWEET_ID', 'term':'TERM', 'emotion':'EMOTION'}, inplace=True)

host=""
user=""
password=""
database = ""

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")

cursor = cnx.cursor()

column_list = list(association.columns)

failed = pd.DataFrame(columns = column_list)

count_inserted = 0
count_skipped = 0

print('loading tweet data...')
for ind, row in association.iterrows():
    try:
        query = (f"""
        INSERT INTO WORD_ASSOCIATION (TWEET_ID, TERM, EMOTION)
        VALUES (
        "{row[column_list[0]]}"
        ,"{row[column_list[1]]}"
        ,"{row[column_list[2]]}"
        );
        """)
        cursor.execute(query)
        count_inserted+=1
    except: 
        count_skipped+=1
        failed.loc[len(failed.index)] = list(row)
        continue
cnx.commit()

print("length of dataframe: ", len(association))
print("inserted: ", count_inserted)
print("skipped: ", count_skipped)

cursor.execute(f"""
INSERT INTO RUN_LOG (LOAD_TYPE, COMMENT, FILEPATH)
VALUES (
"SECONDARY",
"TWEET_DATA: {count_inserted} LOADED, {count_skipped} SKIPPED",
"VW_TWEET_TEXT"
);
""")

cnx.commit()