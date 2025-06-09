CREATE DATABASE IF NOT EXISTS AerolineaDW;
USE AerolineaDW;

CREATE TABLE IF NOT EXISTS dim_fecha (
  fecha_key        INT            PRIMARY KEY,         
  fecha_cal        DATE           NOT NULL,
  anio             SMALLINT       NOT NULL,
  trimestre        TINYINT        NOT NULL,
  mes_num          TINYINT        NOT NULL,
  mes_nombre       VARCHAR(15)    NOT NULL,
  dia_mes          TINYINT        NOT NULL,
  dia_semana_num   TINYINT        NOT NULL,              -- 0=Mon … 6=Sun
  dia_semana_nom   VARCHAR(10)    NOT NULL,
  es_fin_semana    BIT            NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_ruta (
  ruta_key         INT            AUTO_INCREMENT PRIMARY KEY,
  origen           VARCHAR(5)     NOT NULL,           
  destino          VARCHAR(5)     NOT NULL,
  origen_region    VARCHAR(30),
  destino_region   VARCHAR(30),
  distancia_km     INT,
  UNIQUE KEY uq_ruta (origen, destino)
);

CREATE TABLE IF NOT EXISTS dim_clase (
  clase_key        TINYINT        PRIMARY KEY,
  clase_nombre     VARCHAR(20)    NOT NULL
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS dim_estado (
  estado_key       TINYINT        PRIMARY KEY,
  estado_nombre    VARCHAR(20)    NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_clientes (
  id_cliente_pk    INT            AUTO_INCREMENT PRIMARY KEY,
  id_cliente       VARCHAR(10)    NOT NULL,
  nombre           VARCHAR(100),
  pais             VARCHAR(50),
  nivel_frecuencia VARCHAR(20),
  UNIQUE KEY uq_cliente (id_cliente)
);

CREATE TABLE IF NOT EXISTS dim_vuelo (
  id_vuelo_pk      INT            AUTO_INCREMENT PRIMARY KEY,
  id_vuelo         VARCHAR(10)    NOT NULL,
  origen           VARCHAR(5),
  destino          VARCHAR(5),
  clase            VARCHAR(20),
  UNIQUE KEY uq_vuelo (id_vuelo)
);

CREATE TABLE IF NOT EXISTS fact_vuelo (
  id_vuelo            VARCHAR(10)  PRIMARY KEY,      
  fecha_key           INT          NOT NULL,
  ruta_key            INT          NOT NULL,
  clase_key           TINYINT      NOT NULL,
  estado_key          TINYINT      NOT NULL,
  asientos_totales    SMALLINT     DEFAULT 0,
  asientos_ocupados   SMALLINT     DEFAULT 0,
  minutos_retraso     SMALLINT     DEFAULT 0,
  cancelado_flag      BIT          DEFAULT 0,

  CONSTRAINT fk_fact_fecha   FOREIGN KEY (fecha_key)  REFERENCES dim_fecha  (fecha_key),
  CONSTRAINT fk_fact_ruta    FOREIGN KEY (ruta_key)   REFERENCES dim_ruta   (ruta_key),
  CONSTRAINT fk_fact_clase   FOREIGN KEY (clase_key)  REFERENCES dim_clase  (clase_key),
  CONSTRAINT fk_fact_estado  FOREIGN KEY (estado_key) REFERENCES dim_estado (estado_key)
);
