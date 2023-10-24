import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = {
    'owner': 'minhthy',
    'start_date': datetime(2023, 10, 23, 10, 00)
}


def get_data(file):
    data = pd.read_csv(file)   
    return data

data = get_data('/income_classification/income.csv')
 
def transform_df(data): 

# Task 1: Calculate the highest income user's details
    highest_income_user = data[data['target_income'] == '>50K'].sort_values('capital_gain', ascending=False).iloc[0]
    highest_income_user_id = highest_income_user['id']
    highest_income_age = highest_income_user['age']
    highest_income_class = highest_income_user['target_income']
    highest_income_job = highest_income_user['job']

    print("Highest Income User Details:")
    print(f"User ID: {highest_income_user_id}")
    print(f"Age: {highest_income_age}")
    print(f"Income Class: {highest_income_class}")
    print(f"Job: {highest_income_job}")
    print()
    # Create a new DataFrame with the highest income user's details
    highest_income_user_df = pd.DataFrame({
        'User ID': [highest_income_user_id],
        'Age': [highest_income_age],
        'Income Class': [highest_income_class],
        'Job': [highest_income_job]
    })

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = transform_df(data)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_file',
        python_callable=stream_data
    )
