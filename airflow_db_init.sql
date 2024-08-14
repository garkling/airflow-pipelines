CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD '***';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL ON SCHEMA public TO airflow_user;
GRANT USAGE ON SCHEMA public TO airflow_user;

CREATE DATABASE weather;