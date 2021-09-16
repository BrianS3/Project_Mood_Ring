# This file is an example of how to extract tweets without using postman



# For sending GET requests from the API
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
# For dealing with json responses we receive from the API
import json
# For displaying the data after
import pandas as pd
# For saving the response data in CSV format
import csv
# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
#To add wait time between requests
import time


os.environ['TOKEN'] = ''

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from

    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "trump election lang:en" # replace with our keyword(s)
start_time = "2018-11-08T00:00:00.000Z" #replace with our timeframe
end_time = "2018-11-09T00:00:00.000Z" #replace with our timeframe
max_results = 500 # replace with our max

url = create_url(keyword, start_time,end_time, max_results)
json_response = connect_to_endpoint(url[0], headers, url[1])

# print(json.dumps(json_response, indent=4, sort_keys=True))

jsonFile = open("trump_election.json", "w")
jsonFile.write(json.dumps(json_response))
jsonFile.close()






# #Inputs for tweets
# bearer_token = auth()
# headers = create_headers(bearer_token)
# keyword = "xbox lang:en"
# start_list =    ['2021-01-01T00:00:00.000Z',
#                  '2021-02-01T00:00:00.000Z',
#                  '2021-03-01T00:00:00.000Z']

# end_list =      ['2021-01-31T00:00:00.000Z',
#                  '2021-02-28T00:00:00.000Z',
#                  '2021-03-31T00:00:00.000Z']
# max_results = 500

# #Total number of tweets we collected from the loop
# total_tweets = 0

# # Create file
# csvFile = open("data.csv", "a", newline="", encoding='utf-8')
# csvWriter = csv.writer(csvFile)

# #Create headers for the data you want to save, in this example, we only want save these columns in our dataset
# csvWriter.writerow(['author id', 'created_at', 'geo', 'id','lang', 'like_count', 'quote_count', 'reply_count','retweet_count','source','tweet'])
# csvFile.close()

# for i in range(0,len(start_list)):

#     # Inputs
#     count = 0 # Counting tweets per time period
#     max_count = 100 # Max tweets per time period
#     flag = True
#     next_token = None
    
#     # Check if flag is true
#     while flag:
#         # Check if max_count reached
#         if count >= max_count:
#             break
#         print("-------------------")
#         print("Token: ", next_token)
#         url = create_url(keyword, start_list[i],end_list[i], max_results)
#         json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
#         result_count = json_response['meta']['result_count']

#         if 'next_token' in json_response['meta']:
#             # Save the token to use for next call
#             next_token = json_response['meta']['next_token']
#             print("Next Token: ", next_token)
#             if result_count is not None and result_count > 0 and next_token is not None:
#                 print("Start Date: ", start_list[i])
#                 append_to_csv(json_response, "data.csv")
#                 count += result_count
#                 total_tweets += result_count
#                 print("Total # of Tweets added: ", total_tweets)
#                 print("-------------------")
#                 time.sleep(5)                
#         # If no next token exists
#         else:
#             if result_count is not None and result_count > 0:
#                 print("-------------------")
#                 print("Start Date: ", start_list[i])
#                 append_to_csv(json_response, "data.csv")
#                 count += result_count
#                 total_tweets += result_count
#                 print("Total # of Tweets added: ", total_tweets)
#                 print("-------------------")
#                 time.sleep(5)
            
#             #Since this is the final request, turn flag to false to move to the next time period.
#             flag = False
#             next_token = None
#         time.sleep(5)
# print("Total number of results: ", total_tweets)






# In[ ]:




