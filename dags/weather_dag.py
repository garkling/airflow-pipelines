import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable, xcom
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

API_VERSION = "3.0"
CITIES = ["Lviv", "Kyiv", "Kharkiv", "Odesa", "Zhmerynka"]


def parse_temperature_v3(ti: xcom, city: str):
    body = ti.xcom_pull(f"extract_data_{city}")
    data = body['data'][0]

    return dict(
        ts=data['dt'],
        temp=data['temp'],
        humidity=data['humidity'],
        cloudiness=data['clouds'],
        wind_speed=data['wind_speed'],
        city=city.capitalize()
    )


def parse_coo_from_geocode(res):
    data = json.loads(res.text)
    return data[0]['lat'], data[0]['lon']


with DAG(
        dag_id="weather_dag",
        schedule_interval="@hourly",
        start_date=datetime(2024, 8, 13)) as dag:

    table_creation = SQLExecuteQueryOperator(
        task_id="create_table_postgres",
        conn_id="airflow_postgres_conn",
        database="weather",
        sql="""
            CREATE TABLE IF NOT EXISTS measures (
            timestamp BIGINT,
            city VARCHAR(50),
            temp FLOAT,
            humidity INTEGER,
            cloudiness INTEGER,
            wind_speed FLOAT,
            PRIMARY KEY (timestamp, city)
            )
        """
    )

    for city in CITIES:
        city_lc = city.lower()
        geocode = SimpleHttpOperator(
            task_id=f"geocode_{city_lc}",
            http_conn_id="weather_conn",
            endpoint=f"geo/1.0/direct",
            data=dict(appid=Variable.get("WEATHER_API_KEY"), q=city),
            method="GET",
            response_filter=parse_coo_from_geocode,
            log_response=True
        )

        extract_data = SimpleHttpOperator(
            task_id=f"extract_data_{city_lc}",
            http_conn_id="weather_conn",
            endpoint=f"data/{API_VERSION}/onecall/timemachine",
            data=dict(
                appid=Variable.get("WEATHER_API_KEY"),
                lat="""{{ ti.xcom_pull('geocode_%s')[0] }}""" % city_lc,
                lon="""{{ ti.xcom_pull('geocode_%s')[1] }}""" % city_lc,
                dt="""{{ execution_date.int_timestamp }}"""
            ),
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True
        )

        transform_data = PythonOperator(
            task_id=f"transform_data_{city_lc}",
            python_callable=parse_temperature_v3,
            op_kwargs=dict(city=city_lc)
        )

        insert_data = SQLExecuteQueryOperator(
            task_id=f"insert_data_{city_lc}",
            conn_id="airflow_postgres_conn",
            database="weather",
            sql="""
                INSERT INTO measures (timestamp, temp, humidity, cloudiness, wind_speed, city)
                VALUES (
                    {{ ti.xcom_pull('transform_data_%(city)s')['ts']            }},
                    {{ ti.xcom_pull('transform_data_%(city)s')['temp']          }},
                    {{ ti.xcom_pull('transform_data_%(city)s')['humidity']      }},
                    {{ ti.xcom_pull('transform_data_%(city)s')['cloudiness']    }},
                    {{ ti.xcom_pull('transform_data_%(city)s')['wind_speed']    }},
                   '{{ ti.xcom_pull('transform_data_%(city)s')['city']          }}'
                )
                ON CONFLICT (timestamp, city) 
                DO NOTHING
            """ % dict(city=city_lc),
        )

        table_creation >> geocode >> extract_data >> transform_data >> insert_data
