# How to use the contents of this folder:

This folder contains the postman collection used to extract tweets by keyword and data. The postman collection requires an API bearer token. This can be obtained by applying for developer access (https://developer.twitter.com/en/apply-for-access). Please note that accounts that do not have research level access can only obtain tweets from the last 7 calendar days.

## Step 1: Download Postman

The Postman application can be downloaded from the following link https://www.postman.com/downloads/ 

## Step 2: Import Collection

With the Postman application fully installed the following steps should be taken to imprt the collection.

1) Download postman collection from this repo
2) Open Postman
3) Select "File" and then "import"
4) Select "Upload Files"
5) Navigate to folder "
fb752609-10ba-49b0-9918-f764e2405f76"
6) Navigate to "collection"
7) Select "db194a0f-2472-4675-80ab-3a2d3cef1439" and import collection

## Step 3: Setting API Bearer Token

The bearer token included in this collection is not real and will need to be set with your developer application bearer token.

1) Select the folder "Search Tweets" from the collection "Twitter API v2"
2) Select the GET request "Full-archive search"
3) Select "Headers" in GET request
4) Replace "VALUE" with "Bearer [your token]"

## Step 4: Running GET Request

1) Set query parameter: The Key "query" contains the search value for your GET request. Change "Value" to the keyword you wish to search tweets by.
2) Set start_time/end_time: The date/time is  ISO-8601 format, and is the date range that tweets generate up to, but not included the end date. Set the date and time appropriate for your search.
3) Set max_results: Specify in "Value" the total number of results you wish to extract, the maximum value is 500 per request.
4) Click on the white arrow next to "Send" and choose "Send and Download" to retain your results.
 