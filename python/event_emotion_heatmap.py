#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import warnings
warnings.filterwarnings('ignore')


# In[ ]:


import pandas as pd
import mysql
from mysql.connector import connect, Error
import re
from sklearn.preprocessing import MultiLabelBinarizer
import pandas as pd
import numpy as np
import altair as alt


# In[ ]:


host = "162.241.224.47"
user = ""
password = ""
database = "easydau0_bbsiads591"

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")

cursor = cnx.cursor()


# In[ ]:


query = """
SELECT 
EDS.TWEET_ID,
EDS.EMOTION,
S.KEYWORD,
E.EVENT_NAME
FROM VW_EMOTION_DISTINCT_SETS EDS
JOIN TWEET_DATA TD ON EDS.TWEET_ID = TD.TWEET_ID
JOIN SEARCHES S ON S.SEARCH_ID = TD.SEARCH_ID
JOIN EVENTS E ON E.EVENT_ID = S.EVENT_ID
"""

emotions_all = pd.read_sql(query, cnx)


# In[ ]:


def enter_the_matrix2(df):
    '''
    Almost the same as enter_the_matrix, except it indexes on event_name instead of tweet_id
    '''
    
    wlex = pd.read_csv('project/emotion_lexicon_word_level.csv')
    emotions = list(wlex.emotion.unique())
    
    emotions_copy = df
    for e in emotions:
        emotions_copy[e] = emotions_all.EMOTION.str.count(e)
        emotions_copy.replace(2,1)
    emotions_copy.set_index('EVENT_NAME', inplace=True)
    emotion_matrix = emotions_copy[emotions]
    
    return emotion_matrix

    
def get_sum_by_event(event):
    '''
    INPUT: event column name
    DEPENDENCIES: enter_the_matrix2
    PROCESS: gets sums of each emotion per event
    OUTPUT: df with all emotion sums for specified event
    '''
    # creating matrix 
    emotions_all = pd.read_sql(query, cnx)
    dfa = emotions_all
    event_emo_matrix = enter_the_matrix2(dfa)
    event_emo_matrix.reset_index(inplace = True)

    
    b = {key: event_emo_matrix .loc[value] for key, value in event_emo_matrix .groupby("EVENT_NAME").groups.items()}
    
    emotions2 = pd.DataFrame(['anger','anticipation','disgust','fear','joy','negative','positive','sadness','surprise','trust'])
    emotions2.rename(columns = {0:'emotion'}, inplace = True)


    df = pd.DataFrame(b[event])
    df.drop('EVENT_NAME', axis = 1, inplace = True)
    scores = []
    for col in df:
        scores.append(sum(df[col]))
    scores_df = pd.DataFrame(scores)
    scores_df.rename(columns = {0:'sum'}, inplace = True)

    event_sums = emotions2.join(scores_df)
    event_sums['event'] = event

    return event_sums


def make_my_heatmap():
    '''
    DEPENDENCIES:get_sum_by_event
    PROCESS: Creates a heatmap out of all 5 news events associated with sum of each emotion per event
    OUTPUT: heatmap visualzation
    '''
    
    ebola_sums = get_sum_by_event('EBOLA OUTBREAK')
    trumpelection_sums = get_sum_by_event('TRUMP ELECTION')
    sandyhook_sums = get_sum_by_event('SANDY HOOK')
    samesexmarriage_sums = get_sum_by_event('SAME SEX MARRIAGE')
    trumpimpeachment_sums = get_sum_by_event('TRUMP IMPEACHMENT')
    
    sums1 = pd.concat([ebola_sums, trumpelection_sums], axis=0)
    sums2 = pd.concat([sums1, sandyhook_sums], axis = 0)
    sums3 = pd.concat([sums2, samesexmarriage_sums], axis = 0)
    all_sums = pd.concat([sums3, trumpimpeachment_sums], axis = 0)
    

    h = alt.Chart(
    all_sums,
    title='Emotions by Event'
    ).mark_rect().encode(
    x='emotion:N',
        y='event:N',
    color=alt.Color('sum:Q', scale=alt.Scale(scheme="greys",)),
    ).properties(width=400, height = 200
    ).configure_axisX(labelFontSize=12)
    
    return h
    


# In[ ]:


make_my_heatmap()

