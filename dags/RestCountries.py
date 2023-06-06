from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import json
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_country_info():
    url = 'https://restcountries.com/v3.1/all'
    re = requests.get(url)
    data = re.json()

    records = []
    for i in data:
        records.append([i['name']['official'], i['population'], i['area']])        
    
    return records

@task
def load(schema, table, records):
    logging.info("load started...")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country text,
    population int,
    area float
);""")
        for r in records:
            # 따옴표 관련 issue 발생 - values를 분리하여 해결
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s);"
            values = (r[0], r[1], r[2])
            print(values)
            cur.execute(sql, values)
        cur.execute("COMMIT;")
    except Exception as error:
        print(f'error occured. {error}')
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")

with DAG(
    dag_id = 'RestCountries',
    start_date = datetime(2023,5,1),
    catchup=False,
    schedule= '30 6 * * 6' # 분, 시, 일자, 월, 요일
)as dag:

    results = get_country_info()
    load("Hongcana", "country_info", results)
