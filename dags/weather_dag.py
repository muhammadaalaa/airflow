
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json 
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import s3fs

def kelvin_to_fahrenheit(temp_in_kelvin):
    return (temp_in_kelvin - 273.15) * (9/5) + 32

def transform_load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids="extract_weather_data")
    print("Data received: ", data)  

    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_farenheit,
        "Feels Like (F)": feels_like_farenheit,
        "Minimum Temp (F)": min_temp_farenheit,
        "Maximum Temp (F)": max_temp_farenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed
    }
    country = "egypt"
    aws_credentials = {"key": "*****", "secret": "****************", "token": "****"}
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data' + dt_string
    df_data.to_csv(f"s3://etll/{dt_string}.csv", index=False, storage_options=aws_credentials)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}
country = "india"
with DAG(
    'weather_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
    )as dag:

    is_weather_api_ready = HttpSensor(
        
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=' + country + 'f****************'
    )

    extract_weather_data = HttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=' + country + 'f',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True,
        do_xcom_push=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data,
        provide_context=True
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data


