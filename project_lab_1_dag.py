import requests
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator


#my_value = Variable.get("my_key", deserialize_json=True)
#print(my_value)
#Variable.get(f"my_list_key", [], deserialize_json=True)

my_task = PythonOperator(
        task_id="my_dummy_task",
        python_callable=my_task_func,
        provide_context=True,
    )
my_task_two = PythonOperator(
    task_id="my_dummy_task_2",
    python_callable=my_task_func_2,
    provide_context=True,
)



get_twitter_api_data_task = PythonOperator(
    task_id="twitter_api_task",
    python_callable=get_twitter_api_data_task_func,
    provide_context=True
)

def get_auth_header():
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN", deserialize_json=True)
    return {"Authorization": f"Bearer {my_bearer_token}"}

def get_twitter_api():
    user_ids = Variable.get("TWITTER_USER_IDS", deserialize_json=True)
    tweet_ids = Variable.get("TWITTER_TWEET_IDS", deserialize_json=True)
    user_requests = [requests.get(f"https://api.twitter.com/2/users/{id}?user.fields=public_metrics,profile_image_url,username,id,description", headers=get_auth_header()) for id in user_ids]
    tweet_requests = [requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=author_id,text,public_metrics", headers=get_auth_header()) for id in tweet_ids]
    ti.xcom_push("user_requests", user_requests)
    ti.xcom_push("tweet_requests", tweet_requests)
    logging.info(user_requests, tweet_requests)



#def get_auth_header():
  #my_bearer_token = "AAAAAAAAAAAAAAAAAAAAAGrdlQEAAAAAzVmWY9pqSyvsR0QLUVisknVWiRU%3DHebGz3s9dv8b2uo3oOiaBol3HE0287o3DPpqml0G7BCw9xcVY6"
  #return {"Authorization": f"Bearer {my_bearer_token}"}



def my_task_func(ti: TaskInstance, **kwargs):
  my_list = [1,2,3,4,5]
  ti.xcom_push("i_love_ds", my_list)

def my_task_func_2(ti: TaskInstance, **kwargs):
  my_list = ti.xcom_pull(key="i_love_ds", task_ids="my_dummy_task")
  logging.info(my_list)
  #Should log the list [1,2,3,4,5] to this task's log.

with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as gad: 
    my_dummy_task
    my_dummy_task_2
    twitter_api_task


my_dummy_task >> my_dummy_task_2 >> twitter_api_task


#user_id = "44196397"
#api_url = f"https://api.twitter.com/2/users/{user_id}"
#request = requests.get(api_url, headers=get_auth_header())
#print(request.json())
