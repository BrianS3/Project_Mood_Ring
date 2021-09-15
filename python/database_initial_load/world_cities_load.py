#%%
import pandas as pd
import numpy as np
import json
import re
import os
from mysql.connector import connect, Error
import mysql

file_world_cities = os.path.abspath(r"C:\Users\brian\Desktop\git repos\bseko-gdrown-pmurphy\database_facts\worldcities.xlsx")

world_cities = pd.read_excel(file_world_cities)

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")
cursor = cnx.cursor()

uss_query = """
select * from US_STATES
"""

us_states = pd.read_sql_query(uss_query, cnx)
cnx.close()

world_cities['admin_name'] = world_cities['admin_name'].astype(str)
world_cities['admin_name'] = world_cities['admin_name'].apply(lambda x: x.upper())

areas = pd.merge(world_cities, us_states, how='outer', left_on='admin_name', right_on='STATE')
areas.drop(['STATE', 'STATE_ABBR', 'DIV_ID'], axis=1, inplace=True)

areas.rename(columns={'city':'CITY', 'city_ascii':'CITY_ASCII', \
    'lat':'LAT','lng':'LNG','country':'COUNTRY', 'iso2':'ISO2', 'iso3':'ISO3',\
    'admin_name':'STATE_PROVINCE', 'capital':'CAPITAL', \
    'population':'POPULATION', 'id':'CITY_ID'}, inplace=True)

areas['STATE_ID'] = areas['STATE_ID'].fillna(0)

#%%
cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")
cursor = cnx.cursor()

column_list = ['CITY', 'CITY_ASCII', 'LAT', 'LNG', \
    'COUNTRY', 'ISO2', 'ISO3', 'STATE_PROVINCE','CAPITAL',\
    'POPULATION', 'CITY_ID', 'STATE_ID']

count_skipped = 0
count_inserted = 0

failed = pd.DataFrame()

for ind, row in areas.iterrows():
    try:
        query = (f"""
        INSERT INTO CITIES
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
        );
        """)
        cursor.execute(query)
        count_inserted+=1
        print(f'row{count_inserted}')
    except:
        count_skipped+=1
        failed.append(row)
        continue
cnx.commit()

print("length of dataframe: ", len(areas))
print("inserted: ", count_inserted)
print("skipped: ", count_skipped)

# %%
