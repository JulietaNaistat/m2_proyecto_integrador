### Pipeline ELT Medallion para Airbnb

Este proyecto implementa un pipeline ELT de extremo a extremo usando la arquitectura Medallion (bronze - silver - gold) para procesar el dataset de Airbnb NYC y tasas de cambio diarias. Está orquestado con Apache Airflow y persiste en PostgreSQL. El objetivo es obtener tablas listas para análisis y una vista desnormalizada para dashboards o modelos de ML.

### Componentes principales
- **Orquestación**: Apache Airflow (CeleryExecutor) mediante Docker Compose
- **Almacenamiento**: PostgreSQL + pgAdmin
- **Procesamiento**: Python (pandas, SQLAlchemy) y SQL
- **Datos externos**: Currency API (`https://api.currencyapi.com/v3/latest`)

### Estructura del repositorio
- `dags/dag_test.py`: DAG principal `dag_ELT_medallion`
- `dags/capa_bronze/`:
  - `consume_api.py`: Consume tasas de cambio (EUR, GBP, ARS) y guarda `Files/currency_data.csv`
  - `connect_to_db_load_csv.py`: Carga todos los CSV en `Files/` al esquema `bronze` en PostgreSQL
  - `global_quality_check.py`: Chequeos básicos de duplicados y nulos sobre los CSV de entrada
  - `creacion_schemas.sql`: Crea la base y los esquemas (`bronze`, `silver`, `gold`)
- `dags/capa_silver/`:
  - `sql_statements.sql`: Normaliza los datos de `bronze` hacia tablas `silver`: `neighbourhoods`, `hosts`, `listings`, `reviews`, `currencies`
  - `ejecutar_sql.py`: Ejecuta el script SQL de la capa silver
  - `airbnb_schema.dbml`: Modelo lógico de la capa silver
- `dags/capa_gold/`:
  - `listings_enriched.sql`: Crea la vista desnormalizada `gold.listings_OBT` (joins de listings, hosts, neighbourhoods, reviews y conversiones de moneda)
  - `gold_summaries.sql`: Construye tablas agregadas: `gold.listing_summary`, `gold.host_summary`, `gold.neighbourhood_summary`
  - `gold_ejecutar_sql.py`: Ejecuta los scripts de la capa gold
- `docker-compose.yaml`: Servicios de Airflow + PostgreSQL y pgAdmin independientes

### Flujo de datos (DAG `dag_ELT_medallion`)
1. `consume_api`: Obtiene tasas de cambio y guarda `Files/currency_data.csv`
2. `global_quality_check`: Reporta duplicados y nulos para los CSV en `Files/`
3. `connect_to_db_load_csv`: Carga todos los CSV de `Files/` en `bronze.*`
4. `silver_ejecutar_sql`: Crea y pobla tablas normalizadas `silver.*` a partir de `bronze.*`
5. `gold_ejecutar_sql`: Crea tablas `gold.*` listas para análisis y la vista desnormalizada

### Salidas
- Tablas: `gold.listing_summary`, `gold.host_summary`, `gold.neighbourhood_summary`
- Vista: `gold.listings_OBT` para BI/ML (incluye precios convertidos a ARS, EUR, GBP)

### Prerrequisitos
- Docker Desktop
- Clave de API de Currency API
- Dataset local en `dags/capa_bronze/Files/` (por ejemplo, `AB_NYC.csv`) — el DAG también genera `currency_data.csv`

### Variables de entorno
Crea un archivo `.env` en la raíz del proyecto (usado por las tareas de Python) con al menos:

```
DB_USER=tu_user
DB_PASSWORD=tu_password
APIKEY_CURRENCY=TU_CURRENCYAPI_KEY
```

Notas:
- Las tareas de Python se conectan a PostgreSQL en `localhost:5432`, base `m2_pi`.
- Ajusta `DB_USER`/`DB_PASSWORD` si cambias las credenciales en `docker-compose.yaml` → servicio `postgres_db`.

### Levantar los servicios
1. Iniciar servicios:
   - `docker compose up -d`
2. Crear base y esquemas (una sola vez):
   - Usa pgAdmin (`localhost:5050`, admin por defecto `admin@admin.com` / `admin123`) o `psql` para ejecutar `dags/capa_bronze/creacion_schemas.sql`, que crea:
     - base de datos `m2_pi`
     - esquemas `bronze`, `silver`, `gold`
3. Coloca los CSV de entrada en `dags/capa_bronze/Files/` (el repo incluye `AB_NYC.csv`).

### Ejecutar el pipeline
1. Abre la UI de Airflow en `http://localhost:8080` (credenciales por defecto `airflow` / `airflow`).
2. Busca el DAG `dag_ELT_medallion` y ejecútalo manualmente.
3. Tras el éxito, explora los resultados en PostgreSQL (tablas y vista de la capa gold).

### Notas y supuestos
- El pipeline espera que exista la base `m2_pi` antes de correr, debe crearse antes con el script provisto.
- La conversión de moneda depende de valores en vivo de CurrencyAPI
- La ingesta de CSV carga cada archivo encontrado en `dags/capa_bronze/Files/` hacia el esquema `bronze`.
