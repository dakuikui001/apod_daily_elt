from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json

def simple_days_ago(days: int) -> datetime:
    return datetime.now() - timedelta(days=days)

with DAG(
    dag_id='NASA_apod_postgres',
    start_date=simple_days_ago(1),
    schedule='@daily',
    catchup=False
) as dag:
    
    ## Step1 : Create the table if it does not exist

    @task
    def create_table():
        ## initialize postgres hook
        postgresHook = PostgresHook(postgres_conn_id='my_postgres_connection')
        ## SQL query to create the table
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS apod_data(
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                url TEXT,
                date DATE,
                media_type VARCHAR(50)
            );
        '''
        postgresHook.run(create_table_query)

    ## Step2 : Extract Nasa API Data(APOD, astronomy picture of the day)
    ## https://api.nasa.gov/planetary/apod?api_key=6cGdMhYxeCPi6ucmkEWHM29m0v3crEhWe6Erx8gL
    extract_apod = HttpOperator(
        task_id = "extract_apod",
        http_conn_id= 'nasa_api',
        endpoint= 'planetary/apod',
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response: response.json()
    )

    ## Step3 : Transform the data(pick the information need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    ## Step4 : Load the data into Postgres
    @task
    def load_data_to_postgres(apod_data):
        postgresHook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_data_query = '''
            INSERT INTO apod_data(title, explanation, url, date, media_type)
            VALUES(%s, %s, %s, %s, %s)
        '''
        postgresHook.run(insert_data_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    ## Step5 : Define the task dependencies
    create_table() >> extract_apod
    response = extract_apod.output
    transform_apod_data=transform_apod_data(response)
    load_data_to_postgres(transform_apod_data)


