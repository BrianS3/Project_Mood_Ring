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

            df_author = pd.json_normalize(bbb['includes']['users'], max_level = 2)
            df_author = df_author[['id', 'created_at', 'location', 'description', 'public_metrics.followers_count', 'public_metrics.following_count', 'public_metrics.listed_count', 'public_metrics.tweet_count', 'verified']]
            df_author.rename(columns={'id':'AUTHOR_ID', 'created_at':'CREATED_AT', 'location':'LOCATION', 'description':'DESCRIPTION', 'public_metrics.followers_count':'FOLLOWERS_COUNT', \
                                    'public_metrics.following_count':'FOLLOWING_COUNT', 'public_metrics.listed_count':'LISTED_COUNT', 'public_metrics.tweet_count' :'TWEET_COUNT', 'verified':'VERIFIED'}, inplace=True)

            column_list = list(df_author.columns)

            df_author['CREATED_AT'] = df_author['CREATED_AT'].astype('datetime64[ns]').dt.date

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
            for ind, row in df_author.iterrows():
                try:
                    try:
                        query = (f"""
                        INSERT INTO USERS
                        VALUES (
                        '{row[column_list[0]]}'
                        ,'{row[column_list[1]]}'
                        ,'{row[column_list[2]]}'
                        ,'{row[column_list[3]]}'
                        ,'{row[column_list[4]]}'
                        ,'{row[column_list[5]]}'
                        ,'{row[column_list[6]]}'
                        ,'{row[column_list[7]]}'
                        ,'{row[column_list[8]]}'
                        );
                        """)
                        cursor.execute(query)
                        count_inserted+=1
                    except:
                        query = (f"""
                        INSERT INTO USERS
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
                        );
                        """)
                        cursor.execute(query)
                        count_inserted+=1
                except:
                    count_skipped+=1
                    failed.loc[len(failed.index)] = list(row)
                    continue
            cnx.commit()

            pathway = os.path.abspath(r"C:\Users\brian\Desktop\git repos\bseko-gdrown-pmurphy\load_failures\{0}_{1}_{2}_{3}.xlsx").format(event_name,keyword,search_start,search_end)

            print("length of dataframe: ", len(df_author))
            print("inserted: ", count_inserted)
            print("skipped: ", count_skipped)

            cursor.execute(f"""
            INSERT INTO RUN_LOG (LOAD_TYPE, COMMENT, FILEPATH)
            VALUES (
            "INITIAL",
            "USERS: {count_inserted} LOADED, {count_skipped} SKIPPED",
            "{file}"
            );
            """)

            cnx.commit()

            failed.to_excel(pathway)
