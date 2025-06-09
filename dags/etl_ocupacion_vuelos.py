from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text

# Conexión
HOST, PORT = "DB_HOST", 3306
USER, PWD  = "DB_USER", "DB_PASSWORD"

SRC_DB  = "AerolineaBI"    
DEST_DB = "AerolineaDW"    

URL_SRC  = f"mysql+pymysql://{USER}:{PWD}@{HOST}:{PORT}/{SRC_DB}"
URL_DEST = f"mysql+pymysql://{USER}:{PWD}@{HOST}:{PORT}/{DEST_DB}"

default_args = {"owner": "airflow", "start_date": datetime(2025, 6, 8)}
dag = DAG(
    dag_id="etl_ocupacion_vuelos",
    default_args=default_args,
    schedule=None,
    catchup=False,
)

def df_from_query(sql: str, engine):
    with engine.begin() as conn:
        res = conn.execute(text(sql))
        return pd.DataFrame(res.fetchall(), columns=res.keys())

def upsert(df, table, engine):
    cols = df.columns.tolist()
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.executemany(sql, df.values.tolist())
        raw.commit()
        cur.close()
    finally:
        raw.close()

def load_ocupacion_vuelos():
    src_engine  = create_engine(URL_SRC,  pool_pre_ping=True)
    dest_engine = create_engine(URL_DEST, pool_pre_ping=True)

    agg_sql = """
        SELECT
            CONCAT(origen, '-', destino)           AS ruta,
            COUNT(*)                               AS vuelos_totales,
            SUM(capacidad)                         AS asientos_disponibles,
            SUM(ocupacion)                         AS asientos_ocupados,
            ROUND(100 * SUM(ocupacion) / NULLIF(SUM(capacidad),0), 2)
                                                 AS porcentaje_ocupacion
        FROM Vuelos
        GROUP BY origen, destino
    """
    df = df_from_query(agg_sql, src_engine)

    with dest_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS EstadisticasVuelos (
                ruta                VARCHAR(101) PRIMARY KEY,
                vuelos_totales      INT,
                asientos_disponibles INT,
                asientos_ocupados   INT,
                porcentaje_ocupacion DECIMAL(6,2)
            )
        """))

    upsert(df, "EstadisticasVuelos", dest_engine)

PythonOperator(
    task_id="load_ocupacion_vuelos",
    python_callable=load_ocupacion_vuelos,
    dag=dag,
)
