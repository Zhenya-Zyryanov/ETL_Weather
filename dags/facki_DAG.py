from datetime import datetime
import requests
import json
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import os
from io import StringIO
import pytz

# Определение аргументов DAG
args = {
    'owner': 'Zhenya',
    'start_date': datetime(2023, 11, 1),
    'provide_context': True
}

# Получение ключа API из переменных Airflow
key_owm = Variable.get("KEY_API_OWM")

# Определение координат для запроса погоды
latitude = 56.326643
longitude = 44.026069


# Определение функции для извлечения данных о погоде
def extract_data(**kwargs):
    ti = kwargs['ti']
    response = requests.get(
        'https://api.openweathermap.org/data/2.5/weather',
        params={
            'lat': latitude,
            'lon': longitude,
            'appid': key_owm,
            'units': 'metric',
            'lang': 'ru',
            'mode': 'json'
        }
    )

    if response.status_code == 200:
        data = response.json()

        ti.xcom_push(key='weather_json', value=data)


# Определение функции для преобразования данных о погоде
def transform_data(**kwargs):
    ti = kwargs['ti']
    dirty_data = ti.xcom_pull(key='weather_json', task_ids=['extract_data'])[0]

    # Удаление ненужных полей
    for item in dirty_data['weather']:
        del item['icon']
        del item['id']
    del dirty_data['main']['temp_min']
    del dirty_data['main']['temp_max']
    del dirty_data['main']['pressure']
    del dirty_data['wind']['deg']

    # Развертывание вложенных структур данных
    data = {
        **dirty_data['main'],
        'visibility': dirty_data['visibility'],
        **dirty_data['wind'],
        **dirty_data['clouds'],
        **dirty_data['weather'][0]
    }

    # Преобразование словаря в DataFrame
    df = pd.DataFrame([data])
    df.rename(columns={
        'temp': 'температура (С)',
        'feels_like': 'Ощущается (С)',
        'humidity': 'Влажность (%)',
        'visibility': 'Видимость (метр.)',
        'speed': 'Скорость ветра (метр./сек.)',
        'all': 'Облачность (%)',
        'main': 'Параметр погоды',
        'description': 'Погодные условия'}, inplace=True)

    # Получение текущего времени в московском часовом поясе
    moscow_tz = pytz.timezone('Europe/Moscow')
    current_time_tmp = datetime.now(moscow_tz)
    current_time = str(current_time_tmp)[:16]
    df['Дата запроса'] = current_time

    # Преобразование DataFrame в строку JSON
    json_data = df.to_json(orient='records')

    ti.xcom_push(key='weather_df', value=json_data)


# Определение функции для загрузки данных в PostgreSQL
def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='weather_df', task_ids=['transform_data'])[0]
    df = pd.read_json(json_data, orient='records')
    hook = PostgresHook(postgres_conn_id="database_PG")
    conn = hook.get_conn()
    cursor = conn.cursor()
    for i in range(len(df)):
        insert_query = f"""INSERT INTO weather VALUES(
            '{df.iloc[i]['температура (С)']}',
            '{df.iloc[i]['Ощущается (С)']}',
            '{df.iloc[i]['Влажность (%)']}',
            '{df.iloc[i]['Видимость (метр.)']}',
            '{df.iloc[i]['Скорость ветра (метр./сек.)']}',
            '{df.iloc[i]['Облачность (%)']}',
            '{df.iloc[i]['Параметр погоды']}',
            '{df.iloc[i]['Погодные условия']}',
            '{df.iloc[i]['Дата запроса']}'
        )"""
        cursor.execute(insert_query)
    conn.commit()


# Определение DAG
with DAG('Fuck_DAG',
         description='load_weather',
         schedule_interval='*/1 * * * *',
         catchup=False,
         default_args=args,
         tags=["pain2"]) as dag:
    # Определение заданий в DAG
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="database_PG",
        sql="""
            CREATE TABLE IF NOT EXISTS weather (
            temperature TEXT NOT NULL,
            feels_like TEXT NOT NULL,
            humidity TEXT NOT NULL,
            visibility TEXT NOT NULL,
            wind_speed TEXT NOT NULL,
            clouds TEXT NOT NULL,
            weather_main TEXT NOT NULL,
            weather_conditions TEXT NOT NULL,
            request_date TEXT NOT NULL);
            """,
    )
    load_data = PythonOperator(task_id="load_data_to_postgres", python_callable=load_data_to_postgres)

    # Определение порядка выполнения заданий
    extract_data >> transform_data >> create_table >> load_data
