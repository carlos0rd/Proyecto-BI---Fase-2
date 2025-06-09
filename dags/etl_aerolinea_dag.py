from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

default_args = {"owner": "airflow", "start_date": datetime(2025, 6, 6)}

dag = DAG(
    dag_id="etl_aerolinea_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
)

# Extract
def extract():
    data = {
        "cliente_id": [1, 2, 3],
        "nombre": ["Juan", "Ana", "Luis"],
        "total_vuelos": [15, 22, 8],
        "destinos_frecuentes": ["Madrid, Lima", "Bogotá", "Cancún"],
    }
    pd.DataFrame(data).to_csv("/tmp/resumen_clientes.csv", index=False)

# Transform
def transform():
    df = pd.read_csv("/tmp/resumen_clientes.csv")
    df["categoria"] = df["total_vuelos"].apply(
        lambda x: "Frecuente" if x >= 10 else "Ocasional"
    )
    df.to_csv("/tmp/resumen_clientes.csv", index=False)

# Load
def load():
    host, port = "DB_HOST", 3306
    user, passwd = "DB_USER", "DB_PASSWORD"
    db = "AerolineaBI"

    url = f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}"
    engine = create_engine(url, pool_pre_ping=True)

    df = pd.read_csv("/tmp/resumen_clientes.csv")

    # Se crea la tabla si no exisste
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS ResumenClientes (
              cliente_id INT PRIMARY KEY,
              nombre VARCHAR(100),
              total_vuelos INT,
              destinos_frecuentes TEXT,
              categoria VARCHAR(50)
            )
        """
            )
        )

    # Upsert con ON DUPLICATE KEY UPDATE  (evitando duplicados)
    rows = df.values.tolist()
    insert_sql = """
        INSERT INTO ResumenClientes
        (cliente_id, nombre, total_vuelos,
         destinos_frecuentes, categoria)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          nombre=VALUES(nombre),
          total_vuelos=VALUES(total_vuelos),
          destinos_frecuentes=VALUES(destinos_frecuentes),
          categoria=VALUES(categoria);
    """

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.executemany(insert_sql, rows)
        raw_conn.commit()
    finally:
        raw_conn.close()

# tareas
extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
load_task = PythonOperator(task_id="load", python_callable=load, dag=dag)

extract_task >> transform_task >> load_task
