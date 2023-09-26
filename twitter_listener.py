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

#To open up a port to forward tweets
import socket 

os.environ['TOKEN'] = "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAADxqlmxSiZLO05fKmfbrX7G3ckqQ%3DCCPSGoWTDF6uu4qdFsncsuOat5GFTTFv5blXPdA4ueK4YLu3gg"

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
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
keyword = "Data_Engineer lang:en"
# set the end time to the current time minus 1 minute
end_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
end_time = end_time.isoformat(timespec='milliseconds') + 'Z'

# set the start time to the current time minus 10 minutes
start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=11)
start_time = start_time.isoformat(timespec='milliseconds') + 'Z'
max_results = 20

url = create_url(keyword, start_time,end_time, max_results)
json_response = connect_to_endpoint(url[0], headers, url[1])

s = socket.socket()
host = "127.0.0.1"
port = 7777
s.bind((host, port))
print("Listening now!")
s.listen(5)
clientsocket, address = s.accept()

for data in json_response['data']:
    tweet_id = data['id']
    tweet_text = data['text']
    created_at = data['created_at']
    if 'geo' in data:
        place_id = data['geo'].get('place_id', '')
        location_info = next((item for item in json_response['includes']['places'] if item['id'] == place_id), {})
        location_name = location_info.get('name', '').encode('utf-8')
    else:
        location_name = ''.encode('utf-8')
    language = data['lang']
    author_id = data['author_id']
    author_info = next(item for item in json_response['includes']['users'] if item['id'] == author_id)
    author_name = author_info['name'].encode('utf-8')
    author_verified = author_info['verified']
    public_metrics = data['public_metrics']
    
    
    # Encode each field in utf-8 format
    tweet_text = tweet_text.encode('utf-8')
    created_at = created_at.encode('utf-8')
    language = language.encode('utf-8')
    author_name = author_name.decode().encode('utf-8')
    location_name = location_name.decode().encode('utf-8')
    public_metrics = json.dumps(public_metrics).encode('utf-8')
    # Construct a dictionary with key-value pairs
    tweet_dict = {
        "tweet_id": tweet_id,
        "tweet_text": tweet_text.decode(),
        "created_at": created_at.decode(),
        "location_name": location_name.decode(),
        "language": language.decode(),
        "author_name": author_name.decode(),
        "author_verified": author_verified,
        "public_metrics": public_metrics.decode()
    }
    
    # Convert dictionary to a JSON string
    row = json.dumps(tweet_dict)
    
    print("Sending:", row)
    clientsocket.send(row.encode('utf-8'))

    
clientsocket.close()