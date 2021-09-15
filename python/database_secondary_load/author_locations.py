import pandas as pd
import numpy as np
import re
import mysql
from mysql.connector import connect, Error

#define dictionary of state names to abbreviations 
state_names = ["Alaska", "Alabama", "Arkansas", "Arizona", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", 
               "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana",  "Maryland", "Massachusetts", "Maine", "Michigan", "Minnesota", 
               "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico",  
               "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", 
               "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
state_names.sort()

sd = dict(zip(state_names, states))
st = dict(zip(states, state_names))


cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")

cursor = cnx.cursor()

#import user data
query  = "SELECT * FROM USERS"
users = pd.read_sql(query,cnx)

#importint state data
query2 = "SELECT STATE_ID, STATE_ABBR FROM US_STATES"
states_df = pd.read_sql(query2, cnx)

#Filter out trailing USA, remove leading cities, translate state names to abbreviations
df2=users.copy()
df2[["LOCATION"]] = df2[["LOCATION"]].replace(',\ USA$', '',regex = True)
df2[["LOCATION"]] = df2[["LOCATION"]].replace('.*,\ (?=[A-Z]{2}$)', '',regex = True)
df2[["LOCATION"]] = df2[["LOCATION"]].replace('.*\ (?=[A-Z]{2}$)', '',regex = True)
df2[["LOCATION"]] = df2[["LOCATION"]].replace('.*,\ ', '',regex = True)
df2[["LOCATION"]] = df2[["LOCATION"]].replace('.*,(?=[A-Z]{2}$)', '',regex = True)
df2[["LOCATION"]] = df2[["LOCATION"]].replace({"LOCATION": sd})

#filter out only US values
df3 = df2.query('LOCATION in @states')

#replacing us state abbreviation with state id from database
states_dict = dict(zip(states_df['STATE_ID'], states_df['STATE_ABBR']))
user_dict = dict(zip(df3['AUTHOR_ID'], df3['LOCATION']))
final_dict = {}

for skey, sval in states_dict.items():
    for ukey, uval in user_dict.items():
        if uval == sval:
            final_dict[str(ukey)] = skey

failed = pd.DataFrame(columns = [['AUTHOR_ID', 'STATE_ID']])

count_inserted = 0
count_skipped = 0

#pushing data to database
print('loading tweet data...')
for key, val in final_dict.items():
    try:
        query = (f"""
        INSERT INTO AUTHOR_LOCATION (AUTHOR_ID, STATE_ID)
        VALUES (
        "{key}"
        ,"{val}"
        );
        """)
        cursor.execute(query)
        count_inserted+=1
    except: 
        count_skipped+=1
        failed.loc[len(failed.index)] = list([key,val])
        continue
cnx.commit()

print("length of dataframe: ", len(final_dict))
print("inserted: ", count_inserted)
print("skipped: ", count_skipped)

cursor.execute(f"""
INSERT INTO RUN_LOG (LOAD_TYPE, COMMENT)
VALUES (
"SECONDARY",
"TWEET_SENT_CAT: {count_inserted} LOADED, {count_skipped} SKIPPED"
);
""")

cnx.commit()

failed.to_excel('author_location_failures.xlsx')

print('file completed')
