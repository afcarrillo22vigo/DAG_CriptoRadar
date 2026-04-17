from datetime import datetime, timedelta
import json
import logging
import os
from airflow import DAG
from airflow.sdk import task
import requests
import pandas as pd
from sqlalchemy import create_engine

"""
El Objetivo: Crear un pipeline analítico que todos los días extraiga 
el estado de las 50 principales criptomonedas, 
limpie los datos y genere un reporte automatizado en base de datos para saber 
qué monedas son más rentables o volátiles.
"""


default_args = {
    "owner": "alvaro_fdez",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "analisis_criptomonedas_new",
    default_args=default_args,
    description="Análisis de Criptomonedas",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    @task(task_id="extraer_datos")
    def capa_bronce_extraer():
        enlace_criptos = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=eur&order=market_cap_desc&per_page=50&page=1"
        file = requests.get(enlace_criptos).json()
        path = "/opt/airflow/files/critos_bronce.json"

        with open(path, "w") as f:
            json.dump(file, f)

        logging.info("\nSe ha guardado el archivo bruto")

        return path

    @task(task_id="limpiar_datos")
    def capa_plata_limpiar(path_json, ds):
        df = pd.read_json(path_json)
        print(df.columns.to_list())
        df = df[df["current_price"] >= 0]

        df_clean = df[
            [
                "id",
                "symbol",
                "current_price",
                "market_cap",
                "total_volume",
                "price_change_percentage_24h",
            ]
        ].copy()
        print(df.columns.to_list())
        print(df_clean.columns.to_list())
        df_clean["Date"] = ds

        print(df_clean.columns.to_list())
        path_csv = "/opt/airflow/files/criptos_plata.csv"
        # file_exists = os.path.isfile(path_csv)
        df_clean.to_csv(path_csv, mode="w", index=False, header=True)

        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 1000)

        logging.info(f"\n{df_clean.head(10)}")
        return path_csv

    @task(task_id="guardar_datos")
    def capa_oro_guardar(path):
        df = pd.read_csv(path)
        postgre_chain = "postgresql+psycopg2://airflow:airflow@postgres/airflow"  # SACADA DEL DOCKER COMPOSE, LÍNEA 60
        engine = create_engine(postgre_chain)

        df.to_sql(name="criptos_oro", con=engine, if_exists="replace", index=True)
        logging.info("\nDatos guardados en PostgreSQL")

    @task.bash
    def transformar_dbt():
        return "cd /opt/airflow/cripto_dbt && dbt test --profiles-dir ."

    path = capa_bronce_extraer()
    path_plata = capa_plata_limpiar(path)
    capa_oro_guardar(path_plata) >> transformar_dbt()
