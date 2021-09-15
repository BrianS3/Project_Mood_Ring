from typing import List
import pandas as pd
import numpy as np
from pandas import json_normalize
import json
import re
import os
import mysql
from mysql.connector import connect, Error
from datetime import date

today = date.today()

events = ['ebola', 'sandy_hook', 'ss_marriage', 'trump_election', 'trump_impeachment']
event_dict = {'ebola':3000, 'sandy_hook':3002, 'trump_election':3001, 'ss_marriage':3003, 'trump_impeachment':3004}

event_pathway = os.path.abspath(r"C:\Users\brian\Desktop\git repos\bseko-gdrown-pmurphy\json")

for event_name in events:
    directory = f'{event_pathway}\{event_name}'

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file = os.path.join(directory, filename)
            substr = re.findall("\W\w+\d.+?['.json']", file)
            keyword_primer = re.findall('[A-z]+.[^0-9]', substr[0])
            keyword = re.findall("\w+[^'_']",keyword_primer[0])[0]
            search_start, search_end = re.findall('[0-9-]+', substr[0])

            event_id = event_dict[event_name]

            print("loading: ",keyword, 'Date Range: ',search_start,"-", search_end)

            with open(file, 'r', encoding='utf8') as datafile:
                bbb = json.loads(datafile.read())

            df_data = pd.json_normalize(bbb['data'], max_level = 5)
            retweet = df_data[df_data['referenced_tweets'].isna()==False]
            retweet['rt_id'] = retweet['referenced_tweets'].apply(lambda x: list(list(x)[0].values())[1])
            retweet = retweet[['id', 'rt_id']]

            df_text = df_data[['id', 'text']]
            df_text_merge = pd.merge(df_text, retweet, how='outer', left_on='id', right_on='id')
            df_retweet = pd.json_normalize(bbb['includes']['tweets'], max_level = 1)
            df_retweet = df_retweet[['id', 'text']]
            df_retweet.rename(columns = {'id':'rt_id_includes', 'text':'rt_text'}, inplace=True)
            df_text_final = pd.merge(df_text_merge, df_retweet, how='outer', left_on='rt_id', right_on='rt_id_includes')
            df_text_final = df_text_final[['id', 'text', 'rt_id', 'rt_text']]
            df_text_final.rename(columns={'id':'TWEET_ID', 'text':'TWEET', 'rt_id':'RETWEET_ID', 'rt_text':'RETWEET_TEXT'}, inplace=True)

            column_list = list(df_text_final.columns)

            failed = pd.DataFrame(columns = column_list)

            host=""
            user=""
            password=""
            database = ""

            cnx = mysql.connector.connect(user=f'{user}', password=f'{password}', host=f'{host}', database=f"{database}")
            cursor = cnx.cursor()
            
            count_inserted = 0
            count_skipped = 0

            print('loading...')
            for ind, row in df_text_final.iterrows():
                try:
                    try:
                        query = (f"""
                        INSERT INTO TWEET_TEXT (TWEET_ID, TWEET, RETWEET_ID, RETWEET_TEXT)
                        VALUES (
                        "{row[column_list[0]]}"
                        ,"{row[column_list[1]]}"
                        ,"{row[column_list[2]]}"
                        ,"{row[column_list[3]]}"
                        );
                        """)
                        cursor.execute(query)
                        count_inserted+=1
                    except:
                        query = (f"""
                        INSERT INTO TWEET_TEXT (TWEET_ID, TWEET, RETWEET_ID, RETWEET_TEXT)
                        VALUES (
                        '{row[column_list[0]]}'
                        ,'{row[column_list[1]]}'
                        ,'{row[column_list[2]]}'
                        ,'{row[column_list[3]]}'
                        );
                        """)
                        cursor.execute(query)
                        count_inserted+=1
                except: 
                    count_skipped+=1
                    failed.loc[len(failed.index)] = list(row)
                    continue
            cnx.commit()

            pathway = os.path.abspath(r"C:\Users\brian\Desktop\git repos\bseko-gdrown-pmurphy\load_failures\text_{0}_{1}_{2}_{3}.xlsx").format(event_name,keyword,search_start,search_end)

            print("length of dataframe: ", len(df_text_final))
            print("inserted: ", count_inserted)
            print("skipped: ", count_skipped)

            cursor.execute(f"""
            INSERT INTO RUN_LOG (LOAD_TYPE, COMMENT, FILEPATH)
            VALUES (
            "INITIAL",
            "TEXT: {count_inserted} LOADED, {count_skipped} SKIPPED",
            "{file}"
            );
            """)

            cnx.commit()

            failed.to_excel(pathway)

 