from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pymysql                 # driver
from sqlalchemy import create_engine, text

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 6),
}

dag = DAG(
    dag_id="etl_estadisticas_clientes",
    default_args=default_args,
    schedule=None,
    catchup=False,
)

# -Tarea unica
def load_estadisticas_clientes():
    host, port   = "DB_HOST", 3306
    user, passwd = "DB_USER", "DB_PASSWORD"
    db           = "AerolineaBI"
    url          = f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}"

    engine   = create_engine(url, pool_pre_ping=True)

    read_sql = """
        SELECT
            cliente_frecuente,
            COUNT(*) AS total
        FROM Clientes
        GROUP BY cliente_frecuente
    """
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(read_sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        df   = pd.DataFrame(rows, columns=cols)
        cur.close()
    finally:
        raw_conn.close()

    # Se normaliza el resultado para tener una unica linea de resumen
    total_clientes      = int(df["total"].sum())
    clientes_frecuentes = int(df.loc[df["cliente_frecuente"] == 1, "total"].sum())
    porcentaje          = round(clientes_frecuentes * 100 / total_clientes, 2) if total_clientes else 0

    # Se crea la tabla destino si no existe
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS EstadisticasClientes (
                id                      TINYINT PRIMARY KEY,
                total_clientes          INT,
                clientes_frecuentes     INT,
                porcentaje_frecuentes   FLOAT
            )
        """))

    # Insertar / Actualizar datos
    insert_sql = """
        REPLACE INTO EstadisticasClientes
        (id, total_clientes, clientes_frecuentes, porcentaje_frecuentes)
        VALUES (1, %s, %s, %s)
    """
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(insert_sql, (total_clientes, clientes_frecuentes, porcentaje))
        raw_conn.commit()
        cur.close()
    finally:
        raw_conn.close()


load_task = PythonOperator(
    task_id="load_estadisticas_clientes",
    python_callable=load_estadisticas_clientes,
    dag=dag,
)
