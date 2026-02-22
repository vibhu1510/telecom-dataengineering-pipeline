-- Create separate databases for Airflow and Superset
CREATE DATABASE superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO airflow;

-- Hive Metastore schema (used by Trino/Spark for Iceberg catalog)
CREATE DATABASE metastore;
GRANT ALL PRIVILEGES ON DATABASE metastore TO airflow;
