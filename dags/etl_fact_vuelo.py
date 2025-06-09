from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import logging

# Conexión
HOST, PORT = "DB_HOST", 3306
USER, PWD  = "DB_USER", "DB_PASSWORD"

SRC_DB = "AerolineaBI"
DW_DB  = "AerolineaDW"

URL_SRC = f"mysql+pymysql://{USER}:{PWD}@{HOST}:{PORT}/{SRC_DB}"
URL_DW  = f"mysql+pymysql://{USER}:{PWD}@{HOST}:{PORT}/{DW_DB}"

default_args = {"owner": "airflow", "start_date": datetime(2025, 6, 8)}
dag = DAG(
    dag_id="etl_fact_vuelo",
    default_args=default_args,
    schedule=None,     
    catchup=False,
    doc_md=__doc__,
)

def df_from_query(sql: str, engine):
    with engine.begin() as conn:
        res = conn.execute(text(sql))
        return pd.DataFrame(res.fetchall(), columns=res.keys())

def upsert(df, table, engine):
    if df.empty:
        logging.warning("[fact_vuelo] No hay filas que insertar / actualizar")
        return

    cols         = df.columns.tolist()
    col_list     = ", ".join(cols)
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

def ensure_fact_table(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS fact_vuelo (
        id_vuelo        VARCHAR(10) PRIMARY KEY,
        fecha_key       INT,
        ruta_key        INT,
        clase_key       TINYINT,
        estado_key      TINYINT,
        asientos_totales   SMALLINT,
        asientos_ocupados  SMALLINT,
        minutos_retraso    SMALLINT,
        cancelado_flag     BIT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def load_fact_vuelos():
    src_eng = create_engine(URL_SRC, pool_pre_ping=True)
    dw_eng  = create_engine(URL_DW,  pool_pre_ping=True)
  
    ensure_fact_table(dw_eng)

    ruta_lu   = df_from_query("SELECT ruta_key, origen, destino FROM dim_ruta",   dw_eng)
    clase_lu  = df_from_query("SELECT clase_key, UPPER(TRIM(clase_nombre)) AS clase FROM dim_clase", dw_eng)
    estado_lu = df_from_query("SELECT estado_key, UPPER(TRIM(estado_nombre)) AS estado FROM dim_estado", dw_eng)
    fecha_lu  = df_from_query("SELECT fecha_key, fecha_cal FROM dim_fecha", dw_eng)

    ruta_map   = {(r.origen, r.destino): r.ruta_key    for r in ruta_lu.itertuples()}
    clase_map  = {c.clase: c.clase_key                for c in clase_lu.itertuples()}
    estado_map = {e.estado: e.estado_key              for e in estado_lu.itertuples()}
    fecha_map  = {f.fecha_cal: f.fecha_key            for f in fecha_lu.itertuples()}

    # Extraer datos de la base OLTP
    vuelos_sql = """
        SELECT
            v.id_vuelo,
            v.origen,
            v.destino,
            v.fecha_salida              AS fecha_src,
            v.clase                     AS clase_src,
            v.estado                    AS estado_src,
            v.capacidad                 AS asientos_totales,
            v.ocupacion                 AS asientos_ocupados,
            COALESCE(r.minutos_retraso, 0) AS minutos_retraso
        FROM Vuelos v
        LEFT JOIN Retrasos r ON r.id_vuelo = v.id_vuelo
    """
    vuelos_df = df_from_query(vuelos_sql, src_eng)

    vuelos_df["fecha_src"] = pd.to_datetime(vuelos_df["fecha_src"]).dt.date
    vuelos_df["clase_src"] = vuelos_df["clase_src"].str.upper().str.strip()
    vuelos_df["estado_src"]= vuelos_df["estado_src"].str.upper().str.strip()

    vuelos_df["ruta_key"]   = vuelos_df.apply(lambda r: ruta_map.get((r.origen, r.destino)), axis=1)
    vuelos_df["clase_key"]  = vuelos_df["clase_src"].map(clase_map)
    vuelos_df["estado_key"] = vuelos_df["estado_src"].map(estado_map)
    vuelos_df["fecha_key"]  = vuelos_df["fecha_src"].map(fecha_map)
    vuelos_df["cancelado_flag"] = (vuelos_df["estado_src"] == "CANCELADO").astype(int)

    fact = vuelos_df.dropna(subset=["ruta_key","clase_key","estado_key","fecha_key"]).copy()

    lost = len(vuelos_df) - len(fact)
    if lost:
        logging.warning(f"[fact_vuelo] {lost} vuelo(s) sin claves; se descartan")

    fact = fact[
        [
            "id_vuelo",
            "fecha_key",
            "ruta_key",
            "clase_key",
            "estado_key",
            "asientos_totales",
            "asientos_ocupados",
            "minutos_retraso",
            "cancelado_flag",
        ]
    ].fillna(0)

    fact["asientos_totales"]  = fact["asientos_totales"].astype(int)
    fact["asientos_ocupados"] = fact["asientos_ocupados"].astype(int)
    fact["minutos_retraso"]   = fact["minutos_retraso"].astype(int)

    upsert(fact, "fact_vuelo", dw_eng)
    logging.info(f"[fact_vuelo] {len(fact)} fila(s) insertadas/actualizadas")

PythonOperator(
    task_id="load_fact_vuelos",
    python_callable=load_fact_vuelos,
    dag=dag,
)
