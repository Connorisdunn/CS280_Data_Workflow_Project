from airflow import DAG
import logging
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models import TaskInstance
import requests
import pandas as pd
from google.cloud import storage
from gcsfs import GCSFileSystem
import json
from databox import Client
import csv

def get_twitter_api(ti: TaskInstance, **kwargs):
    user_ids = Variable.get("TWITTER_USER_IDS", deserialize_json=True)
    tweet_ids = Variable.get("TWITTER_TWEET_IDS", deserialize_json=True)
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    header_token = {"Authorization": f"Bearer {my_bearer_token}"}
    user = [requests.get(f"https://api.twitter.com/2/users/{id}?user.fields=public_metrics,profile_image_url,username,id,description", headers=header_token).json() for id in user_ids]
    tweet = [requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=author_id,text,public_metrics", headers=header_token).json() for id in tweet_ids]
    ti.xcom_push("user", json.dumps(user))
    ti.xcom_push("tweet", json.dumps(tweet))
    logging.info(user)
    logging.info(tweet)

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
    users = data=ti.xcom_pull(key="user", task_ids="get_twitter_api_data_task")
    tweets = data=ti.xcom_pull(key="tweet", task_ids="get_twitter_api_data_task")
    tweet_header_list = ['retweet_count', 'reply_count', 'like_count', 'quote_count', 'impression_count', 'tweet_id', 'text', 'id']
    user_header_list = ['followers_count','following_count','tweet_count','listed_count','name','username','id']
    user_matching_data = iterate_json_list(json.loads(users), user_header_list)
    tweet_matching_data = iterate_json_list(json.loads(tweets), tweet_header_list)

    client = storage.Client()
    bucket = client.get_bucket("c-d-apache-airflow-cs280")
    bucket.blob("data/user.csv").upload_from_string(user_matching_data.to_csv(index=False), "text/csv")
    bucket.blob("data/tweet.csv").upload_from_string(tweet_matching_data.to_csv(index=False), "text/csv")

    user_token = Variable.get("DATABOX_TOKEN")
    dbox = Client(user_token)
    
    fs = GCSFileSystem(project="Connor-Dunn-CS-280")
    with fs.open('gs://c-d-apache-airflow-cs280/data/user.csv', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        data = [row for row in reader]
        test_list = ['followers_count','following_count','listed_count','tweet_count']
        for val in data:
            for idx, item in enumerate(val):
                count = test_list.count(header[idx])
                name_index = header.index('name')
                if count > 0:
                    dbox.push(f'UserMetric {val[name_index]} {header[idx]}', item)
    with fs.open('gs://c-d-apache-airflow-cs280/data/tweet.csv', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        data = [row for row in reader]
        test_list = ['reply_count','like_count','impression_count','retweet_count']
        for val in data:
            for idx, item in enumerate(val):
                count = test_list.count(header[idx])
                tweet_index = header.index('id')
                if count > 0:
                    dbox.push(f'TweetMetric {val[tweet_index]} {header[idx]}', item)

                    
def flatten_json(data_dict, matching_data, keys_to_match):
    for key, value in data_dict.items():
        if type(value) is dict:
            flatten_json(value, matching_data, keys_to_match)
        else:
            if key in keys_to_match:
                matching_data.append(f"{key}: {value}")
    return matching_data

def iterate_json_list(data_dict, keys_to_match):
    match_list = []
    for dicti in data_dict:
        matches = []
        if type(dicti) is dict:
            flatten_json(dicti, matches, keys_to_match)
        matches = {x.split(": ")[0]: x.split(": ")[1] for x in matches}
        matches = dict(sorted(matches.items()))
        match_list.append(matches)
    return pd.DataFrame(match_list)

with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *", # schedule_interval="*/1 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    get_twitter_api_data_task = PythonOperator(
        task_id="get_twitter_api_data_task", 
        python_callable=get_twitter_api,
        provide_context=True
    )
    transform_twitter_api_data_task = PythonOperator(
        task_id="transform_twitter_api_data_task", 
        python_callable=transform_twitter_api_data_func,
        provide_context=True
    )

get_twitter_api_data_task >> transform_twitter_api_data_task
