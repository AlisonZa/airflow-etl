from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from airflow.providers.http.operators.http import SimpleHttpOperator
except ImportError:
    from airflow.providers.http.operators.http import HttpOperator as SimpleHttpOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from pendulum import datetime
import json

# Define the DAG
with DAG(
    dag_id = "nasa_apod_postgres",
    start_date = datetime(2025, 6, 6).subtract(days=1),
    schedule_interval = "@daily",
    catchup = False,  
)as dag:
    
    # Defining the tasks:
    ## Task 1 - Create the table, if the table does not exist
    def create_table():
        ### Initialize the Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id = "my_connection_id") # This name goes to the Airflow UI when setting the connection

        ### Query to create the table:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data(
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
            );       
        """
        ### Execute the Query:
        postgres_hook.run(create_table_query)
    
    # Convert to a task
    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    ## Task 2 - [E]xtract the API Data
    ### This is the following http https://api.nasa.gov/planetary/apod?api_key=<YourApiKeyHere>
    extract_apod = SimpleHttpOperator(
        task_id = "extract_apod",
        http_conn_id = "nasa_api",
        endpoint = "planetary/apod",
        method = "GET",
        data = {"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, # Extract the information from the AirFlow connection (set via UI)
        response_filter = lambda response:response.json(),
    )

    ## Task 3 - [T]ransform the Data
    @task
    def transform_apod_data(response):
        apod_data = {   
            "title": response.get("title", ""),
            "explanation": response.get("explanation", ""),
            "url": response.get("url", ""), 
            "date": response.get("date", ""), 
            "media_type": response.get("media_type", ""), 
        }
        return apod_data

    ## Task 4 - [L]oad the data into the SQL DataBase
    @task
    def load_data_to_postgres(apod_data):
        
        postgres_hook = PostgresHook(postgres_conn_id = "my_connection_id")

        ### Define the insert query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ### Execute the query
        postgres_hook.run(insert_query, parameters=(
            apod_data["title"],
            apod_data["explanation"],
            apod_data["url"],
            apod_data["date"],
            apod_data["media_type"],
        ))

    ## Task 5 - Verify the data
        
    
    ## Define the task dependencies:
    transformed_data = transform_apod_data(extract_apod.output)
    create_table_task >> extract_apod >> transformed_data >> load_data_to_postgres(transformed_data)
