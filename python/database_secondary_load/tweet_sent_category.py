import mysql
from mysql.connector import connect, Error
import pandas as pd
import os


cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")

cursor = cnx.cursor()

query = """
SELECT * FROM VW_TWEET_EMOTIONS_ALL
"""
tweets = pd.read_sql(query, cnx)

def sent(df):
    '''
    Input: dataframe from database with emotions in listagg form or by distinct value.
    Output: datafram with positive/negative sentiment association
    '''
    neg_emotions = ['anger', 'disgust', 'fear', 'sadness', 'negative']
    pos_emotions = ['anticipation', 'joy', 'surprise', 'trust', 'positive']

    for ind, row in df.iterrows():
        pos_count = 0
        neg_count = 0
        for emo in row['EMOTIONS_ALL'].split(', '):
            for nsent in neg_emotions:
                if emo == nsent: 
                    neg_count+=1
            for psent in pos_emotions:
                if emo ==psent: 
                    pos_count+=1
        if pos_count > neg_count: df.at[ind, 'sentiment'] = 1
        elif pos_count == neg_count: df.at[ind, 'sentiment'] = 9
        else: df.at[ind,'sentiment'] = 0

    return df

tweets = sent(tweets)

column_list = list(tweets.columns)

failed = pd.DataFrame(columns = column_list)

count_inserted = 0
count_skipped = 0

print('loading tweet data...')
for ind, row in tweets.iterrows():
    try:
        query = (f"""
        INSERT INTO SENTIMENT_CATEGORY_ASSIGNMENT (TWEET_ID, SENT_ID)
        VALUES (
        "{row[column_list[0]]}"
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

pathway = os.path.abspath(r"C:\Users\brian\Desktop\git repos\bseko-gdrown-pmurphy\load_failures\tweet_sent_category.xlsx")

print("length of dataframe: ", len(tweets))
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

failed.to_excel(pathway)

print('file completed')