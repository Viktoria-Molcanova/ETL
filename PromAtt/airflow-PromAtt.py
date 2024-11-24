from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_data():
    booking_df = pd.read_csv('booking.csv')
    client_df = pd.read_csv('client.csv')
    hotel_df = pd.read_csv('hotel.csv')
    return booking_df, client_df, hotel_df

def transform_data(**kwargs):
    booking_df, client_df, hotel_df = kwargs['ti'].xcom_pull(task_ids='load_data')
    
    # Объединение таблиц
    merged_df = booking_df.merge(client_df, on='client_id').merge(hotel_df, on='hotel_id')
    
    # Приведение дат к одному виду
    merged_df['booking_date'] = pd.to_datetime(merged_df['booking_date'], errors='coerce')
    
    # Удаление некорректных колонок
    merged_df.drop(columns=['currency'], inplace=True)
    
    # Приведение всех валют к одной 
    merged_df['booking_cost'] = merged_df.apply(lambda row: convert_currency(row['booking_cost'], row['currency']), axis=1)
    merged_df['currency'] = 'GBP'  
    
    return merged_df

# Функция для конвертации валют
def convert_currency(amount, currency):
    conversion_rates = {'EUR': 0.85, 'GBP': 1.0} 
    return amount * conversion_rates.get(currency, 1)

# Функция для загрузки данных в базу данных
def load_to_db(**kwargs):
    transformed_df = kwargs['ti'].xcom_pull(task_ids='transform_data')
    engine = create_engine('sqlite:///hotel_bookings.db')
    transformed_df.to_sql('bookings', con=engine, if_exists='replace', index=False)

# Определение DAG
dag = DAG('hotel_booking_dag', description='DAG для обработки данных',
          schedule_interval='@daily', start_date=datetime(2023, 10, 1), catchup=False)

# Определение операторов
load_data_task = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)
transform_data_task = PythonOperator(task_id='transform_data', python_callable=transform_data, provide_context=True, dag=dag)
load_to_db_task = PythonOperator(task_id='load_to_db', python_callable=load_to_db, provide_context=True, dag=dag)

# Установка зависимостей
load_data_task >> transform_data_task >> load_to_db_task
