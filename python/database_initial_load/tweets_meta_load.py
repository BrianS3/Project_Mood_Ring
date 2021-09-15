from typing import List
import pandas as pd
import numpy as np
# from pandas import json_normalize
import json
import re
import os
import mysql
from mysql.connector import connect, Error
from datetime import date
events = ['ebola', 'sandy_hook', 'ss_marriage', 'trump_election', 'trump_impeachment'] #file event name
event_dict = {'ebola':3000, 'sandy_hook':3002, 'trump_election':3001, 'ss_marriage':3003, 'trump_impeachment':3004} #event/event id

event_pathway = os.path.abspath(r"C:\Users\brian\Desktop\git repos\bseko-gdrown-pmurphy\json")

host=""
user=""
password=""
database = ""

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")

cursor = cnx.cursor()


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
            print('-----------------------------------------------------')
            print("loading: ",keyword, 'Date Range: ',search_start,"-", search_end)

            print('searching dates.....')

            query_sd = f"SELECT DISTINCT SEARCH_START, SEARCH_END FROM SEARCH_DATES WHERE SEARCH_START = '{search_start}' AND SEARCH_END = '{search_end}'"

            search_results = pd.read_sql_query(query_sd,cnx)

            if (( str(search_results['SEARCH_START'].astype(str).values) == f"['{search_start}']" ) == False) and\
                (( str(search_results['SEARCH_END'].astype(str).values) == f"['{search_end}']" ) == False):
                print('dates not found, inserting records.....')
                insert_query = (f"""
                INSERT INTO SEARCH_DATES (SEARCH_START, SEARCH_END)
                VALUES ('{search_start}', '{search_end}');
                """)
                cursor.execute(insert_query)
                cnx.commit()
                print(f'records inserted: {search_start}, {search_end}')

                print('logging data insertion.....')
                cursor.execute(f"""
                INSERT INTO RUN_LOG (LOAD_TYPE, COMMENT, FILEPATH)
                VALUES (
                "INITIAL",
                "SEARCH_DATES: '{search_start}','{search_end}'",
                "{file}"
                );
                """)
                cnx.commit()
                print('data insertion logged')
            else: print('dates found, moving on......')

            print('extracting search date id.....')
            sd_id_query = f"SELECT SEARCH_DATE_ID FROM SEARCH_DATES WHERE SEARCH_START = '{search_start}' AND SEARCH_END = '{search_end}'"
            search_id = pd.read_sql_query(sd_id_query, cnx)
            print('search id obtained')

            search_id_insert = str(search_id['SEARCH_DATE_ID'].item())

            query_sch = f"SELECT DISTINCT KEYWORD, SEARCH_DATE_ID, SEARCH_ID, EVENT_ID FROM SEARCHES WHERE SEARCH_DATE_ID = '{search_id_insert}' AND KEYWORD='{keyword}' AND EVENT_ID='{event_id}'"

            search_results = pd.read_sql_query(query_sch,cnx)

            if (( str(search_results['KEYWORD'].astype(str).values) == f"['{keyword}']" ) == False) and\
                (( str(search_results['SEARCH_DATE_ID'].astype(str).values) == f"['{search_id_insert}']" ) == False) and\
                (( str(search_results['EVENT_ID'].astype(str).values) == f"['{event_id}']" ) == False):
                print('search criteria not found, inserting records.....')

                print('creating search record.....')
                cursor.execute(f"""
                INSERT INTO SEARCHES (KEYWORD, SEARCH_DATE_ID, EVENT_ID)
                VALUES ('{keyword}', '{search_id_insert}', '{event_id}');
                """)
                cnx.commit()
                print('search record created')
            else: print('search data found, moving on......')

            print('logging data insertion.....')
            cursor.execute(f"""
            INSERT INTO RUN_LOG (LOAD_TYPE, COMMENT, FILEPATH)
            VALUES (
            "INITIAL",
            "SEARCHES: '{keyword}', '{search_id_insert}', '{event_id}''",
            "{file}"
            );
            """)
            cnx.commit()
            print('data insertion logged')

            print('extracting search id.....')
            sd_id_query2 = f"SELECT SEARCH_ID FROM SEARCHES WHERE SEARCH_DATE_ID = '{search_id_insert}' AND KEYWORD='{keyword}' AND EVENT_ID='{event_id}'"
            print(f'prepping id. SEARCH_DATE_ID={search_id_insert}, KEYWORD={keyword}, EVENT_ID={event_id}')
            search_id_2 = pd.read_sql_query(sd_id_query2, cnx)
            search_id_2_insert = str(search_id_2['SEARCH_ID'].item())
            print('search id obtained')

            with open(file, 'r', encoding='utf8') as datafile:
                bbb = json.loads(datafile.read())

            df_meta = pd.json_normalize(bbb['data'], max_level = 3)
            df_meta = df_meta[['author_id','id','created_at']]
            df_meta['created_at'] = df_meta['created_at'].astype('datetime64[ns]').dt.date
            df_meta['SEARCH_ID'] = search_id_2_insert
            df_meta.rename(columns={'author_id':'AUTHOR_ID','created_at':'CREATED_AT','id':'TWEET_ID'}, inplace=True)
            print('meta data prepped for insertion')
            column_list = list(df_meta.columns)

            failed = pd.DataFrame(columns = column_list)

            count_inserted = 0
            count_skipped = 0

            print('loading tweet data...')
            for ind, row in df_meta.iterrows():
                try:
                    query = (f"""
                    INSERT INTO TWEET_DATA (AUTHOR_ID, TWEET_ID, CREATED_AT, SEARCH_ID)
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
                    count_skipped+=1
                    failed.loc[len(failed.index)] = list(row)
                    continue
            cnx.commit()

            pathway = os.path.abspath(r"C:\Users\brian\Desktop\git repos\bseko-gdrown-pmurphy\load_failures\data_{0}_{1}_{2}_{3}.xlsx").format(event_name,keyword,search_start,search_end)

            print("length of dataframe: ", len(df_meta))
            print("inserted: ", count_inserted)
            print("skipped: ", count_skipped)

            cursor.execute(f"""
            INSERT INTO RUN_LOG (LOAD_TYPE, COMMENT, FILEPATH)
            VALUES (
            "INITIAL",
            "TWEET_DATA: {count_inserted} LOADED, {count_skipped} SKIPPED",
            "{file}"
            );
            """)

            cnx.commit()

            failed.to_excel(pathway)

            print('file completed')