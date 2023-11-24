from datetime import datetime, timedelta
import json
import requests
import decimal
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task

# Define default arguments for the DAG
default_args = {
    'owner': 'Ogechi',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

# Define the DAG with the specified parameters
@dag(
    default_args=default_args,
    dag_id='Bank_of_Canada_Exchange_rate',
    start_date=datetime(2023, 11, 23),
    schedule='0 23 * * 5',
    template_searchpath='/tmp'
)

# Define a taskflow
def taskflow():
    # Define the extract_data from the API
    @task
    def extract_data():
        start_date = '2023-01-01'
        url = 'https://www.bankofcanada.ca/valet/observations/group/FX_RATES_DAILY/json?start_date='
        request = requests.get(url+start_date)
        response_data = json.loads(request.text)
        return response_data

    @task
    def transform_data(response):
        rates = []
        for row in response['observations']:
            rates.append((datetime.strptime(row['d'], '%Y-%m-%d'),
                          decimal.Decimal(row['FXUSDCAD']['v']),
                          decimal.Decimal(row['FXAUDCAD']['v']),
                          decimal.Decimal(row['FXGBPCAD']['v']),
                          decimal.Decimal(row['FXEURCAD']['v'])))
        return rates

    @task
    #Write an SQL INSERRT query and save it in a file
    def load_data(data):
        with open('/tmp/postgres_query.sql', 'w') as f:
            for dat, usd, aud, gbp, eur in data:
                f.write(
                    "INSERT INTO exchange (date, fxusdcad, fxaudcad, fxgbpcad, fxeurcad) \
                    VALUES("f'\'{dat}\', {usd}, {aud}, {gbp}, {eur}'");\n"
                )
    #Create a table in Postgres
    create_table_in_postgres = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_localhost2',
        sql="""
            drop table if exists exchange;
            create table if not exists exchange(
                date timestamp,
                fxusdcad decimal,
                fxaudcad decimal,
                fxgbpcad decimal,
                fxeurcad decimal )
        """
    )
    #Load the data to Postgres
    load_data_to_postgres = PostgresOperator(
        task_id='load_data_into_postgres',
        postgres_conn_id='postgres_localhost2',
        sql='postgres_query.sql'
    )


    load_data(transform_data(extract_data())) >> create_table_in_postgres >> load_data_to_postgres


taskflow()