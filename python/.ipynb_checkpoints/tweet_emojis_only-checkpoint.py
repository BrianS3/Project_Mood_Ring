def tweet_emojis(tweet_df, column):
    '''
    INPUT: df with column that contains clean tweet text;
    creates a df that adds columns for the emoji and demoji of all the emojis present in the df\'s tweet text,
    for use with EmoLex; also include count of emojis represented in tweets.
    OUTPUT: df with only emoji/word associations/emojicounts in tweets
    '''
    import pandas as pd
    import numpy as np
    
    # creates a set out of emo_word_df
    emoji_set = set()
    for emoji_list in emo_word_df['emoji']:
        emoji_set.update(emoji_list)

    def extract_uniq_emojis(text):
        # extracts unique emojis...
        return np.unique([chr for chr in text if chr in emoji_set])
    
    # applies extract_uniq_emojis to the TWEET column in our clean_tweets df
    tweet_df['emojis'] = tweet_df[column].apply(extract_uniq_emojis)
    emoji_df = tweet_df[tweet_df['emojis'].map(lambda d: len(d)) > 0]
   
    # making matrix
    from sklearn.preprocessing import MultiLabelBinarizer
    mlb = MultiLabelBinarizer()
    emoji_matrix = pd.DataFrame(data=mlb.fit_transform(emoji_df.emojis),
                            index=emoji_df.index,
                            columns=mlb.classes_)
    #creating emoji only df
    #creating emoji only df
    tweet_emojis_df = pd.DataFrame(emoji_matrix.columns, columns = ['emoji'])
    tweet_emojis_df = demoji(tweet_emojis_df)
    
    d = {'emoji' : emoji_matrix.columns,
         'count' : emoji_matrix.sum(axis=0)}

    tweet_counts = pd.DataFrame(d)
    tweet_counts.reset_index(drop=True, inplace = True)
    tweet_counts['demoji'] = tweet_emojis_df['demoji']
    
    
    tweet_words = emo_word_df.merge(tweet_counts, on= 'demoji', how='right')
    
    # get rid of 'false' emojis
    tweet_words.dropna(thresh = 5, inplace=True)

    return  tweet_words
