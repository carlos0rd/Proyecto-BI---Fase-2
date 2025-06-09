from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# Parametros de conexión
HOST, PORT   = "DB_HOST", 3306
USER, PASSWD = "DB_USER", "DB_PASSWORD"
SRC_DB       = "AerolineaBI"
DW_DB        = "AerolineaDW"

URL_SRC = f"mysql+pymysql://{USER}:{PASSWD}@{HOST}:{PORT}/{SRC_DB}"
URL_DW  = f"mysql+pymysql://{USER}:{PASSWD}@{HOST}:{PORT}/{DW_DB}"

default_args = {"owner": "airflow", "start_date": datetime(2025, 6, 6)}
dag = DAG(
    dag_id="etl_dimensiones",
    default_args=default_args,
    schedule=None,      # manual / «Trigger DAG»
    catchup=False,
    doc_md=__doc__,
)

def df_from_query(sql: str, engine):
    with engine.begin() as conn:
        res = conn.execute(text(sql))
        return pd.DataFrame(res.fetchall(), columns=res.keys())

def bulk_insert(df, table, cols, engine, mode="ignore"):
    """INSERT masivo con IGNORE (o REPLACE)."""
    if df.empty:
        return
    ph   = ", ".join(["%s"] * len(cols))
    cols_str = ", ".join(cols)
    sql = (
        f"REPLACE INTO {table} ({cols_str}) VALUES ({ph})"
        if mode == "replace" else
        f"INSERT IGNORE INTO {table} ({cols_str}) VALUES ({ph})"
    )
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql, df[cols].values.tolist())
        conn.commit()
        cur.close()
    finally:
        conn.close()

def load_dimensiones():
    src_eng = create_engine(URL_SRC, pool_pre_ping=True)
    dw_eng  = create_engine(URL_DW,  pool_pre_ping=True)

    # dim_fecha
    fechas = pd.date_range("2025-01-01", "2026-12-31", freq="D")
    dim_fecha = pd.DataFrame({
        "fecha_key"     : fechas.strftime("%Y%m%d").astype(int),
        "fecha_cal"     : fechas.date,
        "anio"          : fechas.year,
        "trimestre"     : fechas.quarter,
        "mes_num"       : fechas.month,
        "mes_nombre"    : fechas.strftime("%B"),
        "dia_mes"       : fechas.day,
        "dia_semana_num": fechas.weekday,              # 0 = lunes
        "dia_semana_nom": fechas.strftime("%A"),
        "es_fin_semana" : (fechas.weekday >= 5).astype(int),
    })

    # Extraer datos base
    vuelos_df    = df_from_query(
        "SELECT id_vuelo, origen, destino, clase, estado FROM Vuelos",
        src_eng
    )
    clientes_df  = df_from_query(
        "SELECT id_cliente, nombre, pais, nivel_frecuencia FROM Clientes",
        src_eng
    )

    # dim_ruta
    dim_ruta = (
        vuelos_df[["origen", "destino"]]
        .drop_duplicates()
        .assign(origen_region=None, destino_region=None, distancia_km=None)
    )

    # dimclase y dim_estado
    dim_clase = (
        vuelos_df["clase"].drop_duplicates().sort_values()
        .reset_index(drop=True).to_frame(name="clase_nombre")
        .assign(clase_key=lambda d: d.index + 1)[["clase_key", "clase_nombre"]]
    )
    dim_estado = (
        vuelos_df["estado"].drop_duplicates().sort_values()
        .reset_index(drop=True).to_frame(name="estado_nombre")
        .assign(estado_key=lambda d: d.index + 1)[["estado_key", "estado_nombre"]]
    )

    # dim_clientes
    dim_clientes = (
        clientes_df.drop_duplicates(subset=["id_cliente"])
        .sort_values("id_cliente")
        .reset_index(drop=True)[["id_cliente", "nombre", "pais", "nivel_frecuencia"]]
    )

    # dim_vuelo
    dim_vuelo = (
        vuelos_df.drop_duplicates(subset=["id_vuelo"])
        .sort_values("id_vuelo")
        .reset_index(drop=True)[["id_vuelo", "origen", "destino", "clase"]]
    )

    # crear tablas si no existen
    with dw_eng.begin() as c:
        
        c.execute(text("""CREATE TABLE IF NOT EXISTS dim_fecha (
            fecha_key      INT PRIMARY KEY,
            fecha_cal      DATE,
            anio           SMALLINT,
            trimestre      TINYINT,
            mes_num        TINYINT,
            mes_nombre     VARCHAR(15),
            dia_mes        TINYINT,
            dia_semana_num TINYINT,
            dia_semana_nom VARCHAR(10),
            es_fin_semana  BIT
        )"""))
        c.execute(text("""CREATE TABLE IF NOT EXISTS dim_ruta (
            ruta_key INT AUTO_INCREMENT PRIMARY KEY,
            origen   VARCHAR(5),
            destino  VARCHAR(5),
            origen_region  VARCHAR(30),
            destino_region VARCHAR(30),
            distancia_km   INT,
            UNIQUE KEY uq_ruta (origen, destino)
        )"""))
        c.execute(text("""CREATE TABLE IF NOT EXISTS dim_clase (
            clase_key    TINYINT PRIMARY KEY,
            clase_nombre VARCHAR(20)
        )"""))
        c.execute(text("""CREATE TABLE IF NOT EXISTS dim_estado (
            estado_key    TINYINT PRIMARY KEY,
            estado_nombre VARCHAR(20)
        )"""))
        
        c.execute(text("""CREATE TABLE IF NOT EXISTS dim_clientes (
            id_cliente_pk INT AUTO_INCREMENT PRIMARY KEY,
            id_cliente    VARCHAR(10),
            nombre        VARCHAR(100),
            pais          VARCHAR(50),
            nivel_frecuencia VARCHAR(20),
            UNIQUE KEY uq_cliente (id_cliente)
        )"""))
        c.execute(text("""CREATE TABLE IF NOT EXISTS dim_vuelo (
            id_vuelo_pk INT AUTO_INCREMENT PRIMARY KEY,
            id_vuelo    VARCHAR(10),
            origen      VARCHAR(5),
            destino     VARCHAR(5),
            clase       VARCHAR(20),
            UNIQUE KEY uq_vuelo (id_vuelo)
        )"""))

    # Cargas al DW
    bulk_insert(dim_fecha,    "dim_fecha",    dim_fecha.columns.tolist(), dw_eng, mode="replace")
    bulk_insert(dim_ruta,     "dim_ruta",     ["origen", "destino", "origen_region", "destino_region", "distancia_km"], dw_eng)
    bulk_insert(dim_clase,    "dim_clase",    ["clase_key", "clase_nombre"], dw_eng)
    bulk_insert(dim_estado,   "dim_estado",   ["estado_key", "estado_nombre"], dw_eng)
    bulk_insert(dim_clientes, "dim_clientes", ["id_cliente", "nombre", "pais", "nivel_frecuencia"], dw_eng)
    bulk_insert(dim_vuelo,    "dim_vuelo",    ["id_vuelo", "origen", "destino", "clase"], dw_eng)

PythonOperator(
    task_id="load_dimensiones",
    python_callable=load_dimensiones,
    dag=dag,
)
