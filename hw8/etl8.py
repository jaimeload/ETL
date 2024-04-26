from datetime import datetime, timedelta
import pandas as pd 
import pickle
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def extract_data():
    booking_df = pd.read_csv('booking.csv')
    client_df = pd.read_csv('client.csv')
    hotel_df = pd.read_csv('hotel.csv')

    with open('dfs.pickle', 'wb') as f:
        pickle.dump((booking_df, client_df, hotel_df), f)

def transform_data():
    with open('dfs.pickle', 'rb') as f:
        booking_df, client_df, hotel_df = pickle.load(f)

    df = pd.merge(booking_df, client_df, on='client_id', how='left')
    df = pd.merge(df, hotel_df, on='hotel_id', how='left')

    df['booking_date'] = pd.to_datetime(df['booking_date'], format='%Y/%m/%d', errors='coerce')
    df['booking_date'] = pd.to_datetime(df['booking_date'], format='mixed', errors='coerce')
    df = df.dropna(subset=['booking_cost'])
    df['booking_date'] = df['booking_date'].fillna(pd.Timestamp(1900, 1, 1), inplace=True)

    currency_rates = {'GBP': 1, 'EUR': 0.9}
    df['booking_cost'] = df.apply(lambda row: row['booking_cost'] * currency_rates[row['currency']], axis=1)
    df['currency'] = 'GBP'

    with open('transformed.pickle', 'wb') as f:
        pickle.dump(df, f)

def load_data():
    with open('transformed.pickle', 'rb') as f:
        df = pickle.load(f)

    postgres_conn_id = 'postgres_default' 
    pg_hook = PostgresHook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hotel_bookings (
            client_id INT,
            booking_date DATE,
            room_type VARCHAR,
            hotel_id INT,
            booking_cost FLOAT,
            currency CHAR(3),
            age FLOAT,
            client_name VARCHAR,
            type VARCHAR,
            address VARCHAR
        )
    ''')

    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO hotel_bookings (
                client_id, booking_date, room_type, hotel_id, booking_cost, currency, age, client_name, type, address
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', [row['client_id'], row['booking_date'], row['room_type'], row['hotel_id'], row['booking_cost'], row['currency'],    row['age'], row['client_name'], row['type'], row['address']])
        
    conn.commit()
    conn.close()

with DAG('etl_8', default_args=default_args, schedule_interval=None) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task
