#!/usr/bin/env python
# coding: utf-8

# # Extracts emojis and tweet text to associate with sentiments

# ### These first several functions are required for later...
# ##### Scroll down to "Main Functions" to see the association functions

# In[1]:


# importing libraries

import pandas as pd
import numpy as np
import json
from pandas import json_normalize
import re
import demoji
from sklearn.preprocessing import MultiLabelBinarizer
from mlxtend.frequent_patterns import apriori
import nltk
from nltk import Tree


# In[2]:


# Necessary functions (DEPENDENCIES)

def clean_tweets2(file, column1, column2):
    '''INPUT: CSV file, column name as string
    PROCESS: Removes tweets that are sent when a person posts a video or photo only;
            removes digits and hashtag symbols from tweet text.
    OUTPUT: pandas df'''
    data = file #pd.read_csv(file, encoding = 'utf-8')
    data = data.dropna()
    data2 = data[~data[column1].str.contains("Just posted a")]
    for item in data2[column1]:
        item = re.sub(r'\d*#*', "", item)
        data2.reset_index(inplace=True, drop=True)
    
    return data2[[column1, column2]]


def extract_uniq_emojis2(text):
    '''
    INPUT: string
    DEPENDENCIES: must have emo_word_df defined
    PROCESS: Extracts unique emojis from a string
    OUTPUT: np array of unique values
    '''
    emoji_set = set()
    for emoji_list in emo_word_df['emoji']:
        emoji_set.update(emoji_list)
    return np.unique([chr for chr in text if chr in emoji_set])



def demojize_me(df):
    '''
    INPUT: A df with column named <emoji>;
    PROCESS: Adds a column with the alt code for emoji;
    OUTPUT: modified df
    '''
    import demoji
    j = []
    for item in df['emoji']:
        x = demoji.findall_list(item, True)
        j.append(x)
        
    j = [', '.join(x) for x in j]
    df['demoji'] = j
    
    return df

def emojilib_df():
    '''Creates a df from the EmojiLib JSON file'''
    import demoji
    
    with open('project/emoji-en-US.json', 'r', encoding='utf-8') as datafile:
        emo = json.loads(datafile.read())
        df = pd.DataFrame.from_dict(emo, orient = 'index')
        df.reset_index(inplace=True)
        
        #renaming columns
        col = ['emoji', 'w1', 'w2', 'w3', 'w4', 'w5', 'w6', 'w7', 'w8', 'w9', 'w10', 'w11', 'w12', 'w13', 'w14', 'w15']
        old = ['index', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14,15]
        d = dict(zip(old, col))
        df.rename(columns=d, inplace=True)
        df.drop(0, axis = 1, inplace=True)

        emolib_list = []
        for item in df['emoji']:
            x = demoji.findall_list(item, True)
            emolib_list.append(x)
            
            emolib_list = [''.join(x) for x in emolib_list]
        df['demoji'] = emolib_list
        
        h = df.T
        full = []

        for col in h:
            v = []
            for tweet in h[col][1:16]:
                if tweet is not None and tweet not in v:

                    v.append(tweet)

            full.append(v)
            
        full_df = pd.DataFrame(zip(full, df['demoji']))
        full_df.rename({0:'term', 1:'demoji'}, axis = 1, inplace = True)
        full_df['emoji'] = df['emoji']

    return full_df



def emoji_matrix(csv_file, column1, column2):
    '''
    INPUT: csv, tweet column, id column
    DEPENDENCIES: emoji_df, emojilib_df
    PROCESS: creates a matrix with emojis, term, and id
    OUTPUT: matrix
    '''
    first = emoji_df(csv_file, column1, column2)
    emo_word_df = emojilib_df()
    
    from sklearn.preprocessing import MultiLabelBinarizer
    mlb = MultiLabelBinarizer()
    emo_matrix = pd.DataFrame(data=mlb.fit_transform(first.emoji_y),
                            index=first.index,
                            columns=mlb.classes_)
    emo_matrix['id'] = first['ID']
    emo_matrix['term'] = first['term']
    emo_matrix['emoji'] = first['emoji_y']
    
    return emo_matrix


def clean_matrix2(csv_file, column1, column2):
    '''
    INPUT needs csv file (not DF)
    DEPENDENCIES: clean_tweets2, extract_uniq_emojis2
    PROCESS: cleans text and extracts unique emojis; makes a matrix with counts of
        each represented emoji in the tweet file
    OUTPUT: df matrix 
    '''
    from sklearn.preprocessing import MultiLabelBinarizer
    tweets_df = clean_tweets2(csv_file, column1, column2)
    tweets_df['emoji'] = tweets_df[column1].apply(extract_uniq_emojis2)
    tweets_df = tweets_df[tweets_df['emoji'].map(lambda d: len(d)) > 0]
    
    tweets_df.reset_index(inplace = True, drop = True)
    tweets_df = tweets_df.explode('emoji')
    tweets_df = demojize_me(tweets_df)
    tweets_df = tweets_df.loc[tweets_df["demoji"] != '']
    tweets_df.reset_index(inplace = True, drop = True)
    
    

    mlb = MultiLabelBinarizer()
    c_matrix = pd.DataFrame(data=mlb.fit_transform(tweets_df.emoji),
                                index=tweets_df.index,
                                columns=mlb.classes_)
    return c_matrix


def frequent_itemsets(matrix, min_support=0.005, k=1): #setting up the apriori calculation
    '''
    ***use for text***
    
    INPUT: a matrix
    PROCESS: Calculates frequent itemsets
    OUTPUT: df with support column
    '''
    from mlxtend.frequent_patterns import apriori
    matrix_itemset = apriori(matrix, min_support=min_support, use_colnames=True)
    out = matrix_itemset[matrix_itemset['itemsets'].apply(lambda x: len(x)) == k]
    out2 = out.sort_index(ascending=False,)
    return out2


def frequent_emoji(csv, column1, column2, min_support=0.005, k=1): #setting up the apriori calculation
    '''
    INPUT: csv, tweet column, id column
    DEPENDENCIES: emoji_matrix
    PROCESS: Calculates frequent itemsets
    OUTPUT: df with support column
    '''
    emo_matrix = emoji_matrix(csv, column1, column2)
    matrix = emo_matrix.iloc[:, 0:-3]
    
    
    
    from mlxtend.frequent_patterns import apriori
    matrix_itemset = apriori(matrix, min_support=min_support, use_colnames=True)
    out = matrix_itemset[matrix_itemset['itemsets'].apply(lambda x: len(x)) == k]
    
    out.columns = ['emoji' if y=='itemsets' else y for y in out.columns]
    out['emoji'] = out['emoji'].apply(lambda y: ', '.join(list(y))).astype("unicode")

    return out




def get_emolex():
    '''Creates df out of EmoLex csv'''
    df = pd.read_csv('project/emotion_lexicon_word_level.csv')

    return df


def num_tweets(file, column1, column2):
    '''
    INPUT: CSV file, column name as string
    PROCESS: Removes tweets that are sent when a person posts a video or photo only;
        removes digits and hashtag symbols from tweet text.
    Differs from clean_tweets: digits NOT removed
    OUTPUT: pandas df'''
    
    data = file #pd.read_csv(file)
    data.dropna(inplace=True)
    data = data[~data[column1].str.contains("Just posted a")]
    data[column1]= [re.sub(r'@*#*', "", item) for item in data[column1]]
    data.reset_index(inplace=True, drop=True)
    return data

def get_text_df(tweet_text):
    '''
    INPUT: string of tweet text
    PROCESS: Removes URLS from text
    OUTPUT: df
    '''
    nps = []
    for line in tweet_text['TWEET']:
        line = re.sub(r'https?:\/\/.*', '', line, flags=re.MULTILINE)
        nps.append(sent_parse(line))
    npa = np.array(nps, dtype = 'object')

    p = pd.DataFrame(npa)
    p.rename({0:'term'}, axis = 1, inplace = True)
    p['ID'] = tweet_text['ID']
    return p


def emoji_df(csv_file, column1, column2):
    '''
    INPUT: csv file and the column with the tweet text in it;
    DEPENDENCIES: clean_tweets2, emojilib_df, extract_uniq_emojis2, demojize_me
    PROCESS: creates a df that adds columns for the emoji and demoji of all the emojis present in the df\'s tweet text,
            for use with EmojiLib; also includes tweet id.
    OUTPUT: df with only emoji/word associations/tweet_id in tweets
    '''
    # get EmojiLib df
    emo_word_df = emojilib_df()

    # clean csv file
    tweet_df = clean_tweets2(csv_file, column1, column2)

    # # applies extract_uniq_emojis to the TWEET column in our clean_tweets df
    tweet_df['emoji'] = tweet_df.TWEET.apply(extract_uniq_emojis2)
    emo_df = tweet_df[tweet_df['emoji'].map(lambda d: len(d)) > 0]

    emo_df = emo_df.explode('emoji')
    emo_df = demojize_me(emo_df)
    emo_df = emo_df.loc[emo_df["demoji"] != '']
    emo_df.reset_index(inplace = True, drop = True)

    # #creating intermediary df
    tweet_emojis_df = emo_df[['ID', 'emoji', 'demoji']]

    # # finding words associated with emojis
    tweet_words = emo_word_df.merge(tweet_emojis_df, on= 'demoji', how='right')
    tweet_words = tweet_words.explode('term')
    
    return tweet_words

def get_sentiment_id():
    '''
    Encodes emotions as either positive or negative based on emolex
    '''
    data = {'emotion':['anger', 'anticipation', 'disgust', 'fear', 'joy',
            'sadness', 'surprise', 'trust', 'positive', 'negative'],
            'sent_id': [666, 333, 666, 666, 333, 666, 333, 333, 333, 666]}

    sentiment_id = pd.DataFrame(data)
    return sentiment_id

emo_word_df = emojilib_df()


# In[3]:


# functions for parsing tweets into single words
# FROM SIADS 516

patterns = """
    NP: {<JJ>*<NN*>+}
    {<JJ>*<NN*><CC>*<NN*>+}
    """
NPChunker = nltk.RegexpParser(patterns)

def prepare_text(input):
    sentences = nltk.sent_tokenize(input)
    sentences = [nltk.word_tokenize(sent) for sent in sentences]
    sentences = [nltk.pos_tag(sent) for sent in sentences]
    sentences = [NPChunker.parse(sent) for sent in sentences]
    return sentences


def parsed_text_to_NP(sentences):
    nps = []
    for sent in sentences:
        tree = NPChunker.parse(sent)
        for subtree in tree.subtrees():
            if subtree.label() == 'NP':
                t = subtree
                t = ' '.join(word for word, tag in t.leaves())
                nps.append(t)
    return nps


def sent_parse(input):
    sentences = prepare_text(str(input))
    nps = parsed_text_to_NP(sentences)
    return nps


# In[4]:


# creating a matrix out of the text

def text_matrix(df):
    '''
    INPUT: df
    PROCESS: makes a matrix with counts of each represented term in the tweet file
    OUTPUT: df matrix
    '''
    emolex = get_emolex()
    mlb = MultiLabelBinarizer()
    text_matrix = pd.DataFrame(data=mlb.fit_transform(df.term),
                                index=df.index,
                                columns=mlb.classes_)


    z = text_matrix.columns.intersection(emolex['term'].where(emolex['association'] == 1))
    z = pd.DataFrame(z, columns = ['term'])

    text_matrix_2 = text_matrix.T.reset_index()
    text_matrix_2 = text_matrix_2.rename({'index':'term'}, axis = 1)

    text_matrix_3 = text_matrix_2.merge(z, on = 'term', how = 'right').T
    text_matrix_3.columns = z['term']
    text_matrix_3 = text_matrix_3[1:]
    return text_matrix_3


# ### Main Functions
# 
# ##### Scroll down to "IN ACTION!" to see them work

# In[5]:


# Main association functions

def get_emoji_associations(csv, column1, column2):
    '''
    ***Look at your emoji_matrix first to get loc_start and loc_stop!***
    
    INPUT: csv, tweet column, tweet_id column, first and last emojis in emoji matrix
    DEPENDENCIES: emoji_df, frequent_emoji, emoji_matrix, emojilib_df, get_emolex, tweet_emojis2,
                clean_tweets2, emojilib_df, extract_uniq_emojis2, demojize_me, get_sentiment_id
    PROCESS: combines emojis with their frequency and sentiment to create a new df
    OUTPUT: df
    '''
    a = emoji_df(csv, column1, column2)
    a.rename({'emoji_y':'emoji'}, axis = 1, inplace = True)
    a.drop('emoji_x', axis = 1, inplace = True)


    x = frequent_emoji(csv, column1, column2)
    emolex = get_emolex()
    sentiment_id = get_sentiment_id()



    out = (a.merge(x, left_on='emoji', right_on='emoji'))

    new = out.merge(emolex, on='term', how = 'left')
    new2 = new[new['association'] == 1]
    new2 = new2.sort_values('support', ascending = False)
    new2.reset_index(inplace = True, drop = True)
    new2_sent = new2.merge(sentiment_id, how= 'left', on='emotion')
    
    # new2_sent = new2_sent[new2_sent['emotion'] != 'positive']
    # new2_sent = new2_sent[new2_sent['emotion'] != 'negative']
    
    return new2_sent


def get_text_associations(csv, column1, column2):
    '''
    INPUT: csv, tweet column, tweet_id column,
    DEPENDENCIES: get_emolex, num_tweets, get_text_df, text_matrix
    PROCESS: combines text with their frequency and sentiment to create a new df
    OUTPUT: df
    '''
    # create necessary dfs
    emolex = get_emolex()
    sentiment_id = get_sentiment_id()
    tweet_text = num_tweets(csv, column1, column2)
    text = get_text_df(tweet_text)
    
    # create matrix
    t_mat = text_matrix(text)
    
    # get frequent itemsets
    t_matrix = frequent_itemsets(t_mat)
    t_matrix.columns = ['term' if y=='itemsets' else y for y in t_matrix.columns]
    t_matrix["term"] = t_matrix["term"].apply(lambda y: ', '.join(list(y)))
    t_matrix["ID"] = text['ID']
    
    # merging frequency with emotions
    new_t_matrix = t_matrix.merge(emolex, on='term', how = 'left')
    associa = new_t_matrix[new_t_matrix['association'] != 0]
    associa = associa.sort_values('support', ascending = False)
    associa.reset_index(inplace = True, drop = True)
    associa_sent = associa.merge(sentiment_id, how= 'left', on='emotion')
    
    # associa_sent = associa_sent[associa_sent['emotion'] != 'positive']
    # associa_sent = associa_sent[associa_sent['emotion'] != 'negative']
    
    return associa_sent


# # IN ACTION!

# In[6]:


import mysql
from mysql.connector import connect, Error

# connecting to database
host = "162.241.224.47"
user = ""
password = ""
database = "easydau0_bbsiads591"

cnx = mysql.connector.connect(user=f'{user}', password=f'{password}',
                              host=f'{host}', database=f"{database}")
cursor = cnx.cursor()
query  = "select * from VW_TWEET_TEXT"
tweets = pd.read_sql(query,cnx)
tweets.rename(columns={'TWEET_ID':'ID', 'TWEET_TEXT':'TWEET'}, inplace=True)


# ## EMOJIS

# In[7]:


emoji_assoc = get_emoji_associations(tweets, 'TWEET', 'ID')
emoji_assoc


# ## TEXT

# In[8]:


text_assoc = get_text_associations(tweets, 'TWEET', 'ID')
text_assoc

