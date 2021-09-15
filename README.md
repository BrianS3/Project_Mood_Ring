# About
This git (Project Mood Ring) is an expirement to ETL twitter data using simple, opensource tools. This pipeline analyzes twitter data for emotional sentiment. The database and ETL process can be fully replicated from the contents. 

# License
Use and distribution of this data and datamodel are subject to the terms stated [here](https://github.com/BrianS3/01-quasar-pbmurphy-bseko/blob/main/LICENSE).

# Postman
[Postman](https://www.postman.com/downloads/) was used as the primary extraction tool. Examples how to replicate this payload can be found under "Postman". 


# MySQL Database Server Specifications:
 - Version: 5.6.41-84.1 (Percona Server (GPL), Released 84.1, Revision b380619)
 - Compiled for: Linux (x86_64)
 - Running Since: Thursday May 27 22:02:22 2021
 - Connecter Versions: C++ 8.0.25

# Data Pipeline Principles
Data pipelines are necessary for the continuous cleaning of incoming data, to stage in a useable format reducing the needs to replicate steps. Raw data extracted through APIs requires business rules to guide final use. These include how cardinality will affect queries, data formats, and useable records. To allow for efficient analysis data must undergo an ETL process (extraction, transform, load). This dataflow delivers a payload in a uniform format. During the process business rules are applied on multiple tables informing users how data should be queried. Working with structured data, such as XML and JSON have benefits for the ETL process allowing the capture of metadata.  

# Design
This dataset was delivered in JSON format, and the database was constructed to act as a data warehouse to retain raw input, a data mart to deliver structured clean data in a Bus format for incremental data loads, as well as a hybrid relational database following the Kimball/Star-Schema models.  Surrogate keys were created for additional efficiency, utilizing b-tree indexes. This mapping allowed automatic rejection of records that did not meet the initial load of authors and raw tweet, due to the lacking foreign key. This process further maximized storage to allow for scalability. Data was stored on a shared server utilizing MySQL hosted by BlueHost

![fig1](https://github.com/BrianS3/01-quasar-pbmurphy-bseko/blob/main/images/Bus.png)

# Why a Pipeline is Required with Twitter Data
This project is intended as a snapshot in-time of an ongoing analysis of Twitter data. Large extractions are necessary to generate enough data to obtain reliable results. It is unreasonable for a single set of files to clean, retain, and structure data for analysis. The data must be obtained in regular intervals in such amounts that even in binary format, storage would severely limit querying abilities based around memory heap sizes. 

# Structured for efficiency
The database was designed to maximize space for tweet text, making up most of the storage. Initial load tables link to a central fact table that limits subsequent record insertion. Any data that is replicated in metadata or search criteria is set as dimensional link. This reduces the overall storage needed to retain a data set needing millions of records. This design represents a hybrid of several database types, though closely resembles a cross between Kimball  and the Star-Schema approach, as seen in the entity relational diagram (ERD). Secondary loads were created to refine existing data for specific analysis methods allowing extraction of data at superior speeds. Views were utilized to reduce space by structuring queries that could be used in multiple analysis methods but did not require static recall for use.


![fig3](https://github.com/BrianS3/01-quasar-pbmurphy-bseko/blob/main/images/Star_Schema.png)

![fig2](https://github.com/BrianS3/01-quasar-pbmurphy-bseko/blob/main/images/Kimball.png)


# ERD for This Dataset
![ERD](https://github.com/BrianS3/01-quasar-pbmurphy-bseko/blob/main/images/SIADS591_ERD.png)

# ELT Process
Initial Load: Data abstraction starts with a Postman GET request utilizing the Twitter API environment. The specific GET request used in this process can imported [insert GIT link] for replication. JSON files were named in a manner to support metadata loading. Twitter text was loaded first to eliminate complicated tweets. No cleaning was applied to the raw input, and text was restricted to insert utilizing single or double quotes. This retained 90% of the payload and eliminated the need for further manipulation. Twitter users were then loaded and restricted in the same manner as tweet text. Users are allowed to create an account description; this field was restricted to the same business rules as tweets. Metadata was then extracted from file names and payload JSON. This data was loaded last, so that records without foreign keys to Users or Tweet_Text would fail, limiting space to only useable metadata.
Since the goal of this pipeline is to repeat the ETL as frequently as possible, minimal losses were accepted to favor a fast-loading processes.

# Secondary Load: 
Secondary data loads were created to conform manipulated data to existing facts and effectively capture the relationship of the data with existing metadata structures. These loads were necessary to ensure analysis integrity over time. Building off the design of the initial load failures, records that did not meet a consistent criterion for advanced manipulation were eliminated. Scripting this process and distributing to end-users as a data mart guarantees consistent evaluation of the data and minimized hand coding of data [3 page 280].

# Atomic Load Structure:
Python files were used for data loads, making use of the MySQL library. This connection type was selected due to compatibility with the Linux server version and C++ connector. This library also allowed use of atomic commits that are less optimal with pandas .to_sql() commands. While pandas allows for chunking insertions saving memory heap size, the single table push is less reliable as it performs a single insertion at a time. This does not allow for any record failures, deemed necessary in the initial load phase. By performing individual insert statements, the database can reject records that do not meet field and key definitions. The data is then committed with a single statement, allowing for any failure to halt the entire process. This ensures that when a process is incomplete, data custodians are aware that partial data loads were not completed, allowing a full re-run of the process. These “gates” allow users to backtrack data processing, utilizing scripts and audit records.

# Audit Records
An audit table was created and updated for each load type into the database. Failure records were extracted and saved as CSV to ensure transparency of the ETL integrity. These records were also used to cross refence table sizes post loading, ensuring that data insertions committed as designed.


# Model Scaling
Built for scalability, this data pipeline model can be fully automated. Eliminating postman is necessary for full automation, but the GET request can be completely replicated using the python library requests (see example code). The output file can be coded to match the metadata extraction steps and pull/store JSON files up to 500 tweets per call. This scripting also allows the incremental data succession of events that can be moderated with windows scheduler or crontab.

Using a bat file or bash script each python script can be called in succession to load data, eliminating the need for manual oversight. Users can monitor RUN_LOG data to ensure reliability of the process.
Figures

# Sample Code
```
import requests

url = "https://api.twitter.com/2/tweets/search/all?query=zelenksy&start_time=2019-10-31T00:00:00.000Z&end_time=2019-11-01T00:00:00.000Z&max_results=500&tweet.fields=created_at,geo,text&expansions=attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id&place.fields=contained_within,country,country_code,full_name,geo,id,name&user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld"

payload={}
headers = {
  'Authorization': 'Bearer AAAAAAAAAAAAAAAAAOM2SQEAAAAAHCmS8MW1QHpIaf5%UU%3D9Ub9C5En7n9YwsDuC9bNSOd5fGKjqWNUkYJNa3RCeVp7KY8GHm',
  'Cookie': 'guest_id=v1%3A162795355164662568; personalization_id="v1_Fhb5EiLst1yN4SFfBwkQLg=="'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
```
