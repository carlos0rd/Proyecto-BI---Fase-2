# Documentación del Proceso ETL - Aerolínea BI

Este repositorio contiene los DAGs desarrollados en Apache Airflow para implementar procesos ETL que consolidan información crítica de una aerolínea en una base de datos MySQL y un Data Warehouse llamado `AerolineaDW`.

## Requisitos

- Apache Airflow
- MySQL
- Python 3.10 o superior
- Librerías Python:
  - `pandas`
  - `sqlalchemy`
  - `pymysql`

Instalación recomendada:

```bash
pip install pandas sqlalchemy pymysql
```

---

## Conexión a la Base de Datos

Todos los DAGs usan SQLAlchemy con una cadena de conexión directa, sin uso de variables de entorno:

```python
mysql+pymysql://root:1234@localhost:3306/aerolineadw
```

---

## DAGs Desarrollados

### 1. `etl_dimensiones.py`

**Objetivo:** Extraer y cargar todas las dimensiones del modelo al Data Warehouse.

**Tareas:**
- `load_dimensiones`: Genera las tablas `dim_fecha`, `dim_ruta`, `dim_clase`, `dim_estado`, `dim_clientes` y `dim_vuelo` desde los datos de la base operacional.

**Tablas destino:**
- `dim_fecha`: claves de tiempo para análisis de vuelos.
- `dim_ruta`: relaciones origen-destino.
- `dim_clase`: clase del vuelo (económica, ejecutiva, etc).
- `dim_estado`: estado del vuelo (programado, cancelado, etc).
- `dim_clientes`: información de los clientes.
- `dim_vuelo`: metadatos de vuelos.

---

### 2. `etl_fact_vuelo.py`

**Objetivo:** Cargar la tabla de hechos `fact_vuelo` integrando información de vuelos y retrasos, con claves sustitutas desde las dimensiones.

**Tareas:**
- `load_fact_vuelos`: Mapea y transforma los datos de `Vuelos` y `Retrasos`, y los carga en `fact_vuelo`.

**Tabla destino:** `fact_vuelo`
- `id_vuelo`, `fecha_key`, `ruta_key`, `clase_key`, `estado_key`, `asientos_totales`, `asientos_ocupados`, `minutos_retraso`, `cancelado_flag`

---

### 3. `etl_estadisticas_clientes.py`

**Objetivo:** Calcular estadísticas globales de clientes frecuentes.

**Tareas:**
- `load_estadisticas_clientes`: Calcula el porcentaje de clientes frecuentes y lo guarda en `EstadisticasClientes`.

**Tabla destino:** `EstadisticasClientes`
- `total_clientes`, `clientes_frecuentes`, `porcentaje_frecuentes`

---

### 4. `etl_ocupacion_vuelos.py`

**Objetivo:** Calcular el porcentaje de ocupación promedio por ruta.

**Tareas:**
- `load_ocupacion_vuelos`: Agrega ocupación por ruta y almacena el resumen en `EstadisticasVuelos`.

**Tabla destino:** `EstadisticasVuelos`
- `ruta`, `vuelos_totales`, `asientos_disponibles`, `asientos_ocupados`, `porcentaje_ocupacion`

---

## Automatización

- Todos los DAGs deben están ubicados en la carpeta `~/airflow/dags/` para poder ejecutarse en airflow.
- Pueden ejecutarse manualmente desde la UI de Airflow o programarse periódicamente.
---

## Base de Datos y Data Warehouse

- La base de datos operacional contiene las tablas originales (`Clientes`, `Vuelos`, `Retrasos`).
- La base de datos `AerolineaDW` contiene:
  - Dimensiones: `dim_fecha`, `dim_ruta`, `dim_clase`, `dim_estado`, `dim_clientes`, `dim_vuelo`
  - Tabla hechos: `fact_vuelo`
  - Estadísticas: `ResumenClientes`, `EstadisticasClientes`, `EstadisticasVuelos`

