**Español:**

# 🛒 ETL de e-commerce con Airflow + PostgreSQL

Pipeline batch con Apache Airflow para extraer, transformar y cargar datos de ventas simuladas hacia PostgreSQL, con validación de calidad y buenas prácticas.

## Stack
- Orquestación: Apache Airflow
- Almacenamiento: PostgreSQL
- Contenedores: Docker Compose
- Validación: Great Expectations
- Visualización: Metabase

## Estructura (inicial)
- `dags/` DAGs de Airflow
- `data/raw` datos crudos
- `data/processed` datos transformados
- `sql/` scripts SQL
- `include/` utilidades para Airflow
- `plugins/` plugins de Airflow
- `postgres/init/` seeds/DDL para Postgres
- `logs/` logs de Airflow


**English:**

# 🛒 E-commerce ETL with Airflow + PostgreSQL

Batch pipeline using Apache Airflow to extract, transform, and load simulated sales data into PostgreSQL, following data quality checks and best practices.

## Stack

- Orchestration: Apache Airflow
- Storage: PostgreSQL
- Containers: Docker Compose
- Validation: Great Expectations
- Visualization: Metabase

## Initial Structure

- `dags/` Airflow DAGs
- `data/raw` raw data
- `data/processed` transformed data
- `sql/` SQL scripts
- `include/` Airflow utilities
- `plugins/` Airflow plugins
- `postgres/init/` seeds / DDL for Postgres
- `logs/` Airflow logs