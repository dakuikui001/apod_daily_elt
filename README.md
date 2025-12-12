## Overview
Small Airflow ETL example that pulls the NASA APOD (Astronomy Picture of the Day) API, selects the needed fields, and loads them into Postgres. Uses the Astronomer Runtime as the Airflow base image, with Docker to provide a reproducible local environment.

## Architecture
- `dags/etl.py`: single DAG `NASA_apod_postgres`, scheduled `@daily`, no catchup.
- Task flow: `create_table` ➜ `extract_apod` ➜ `transform_apod_data` ➜ `load_data_to_postgres`.
- Required connections:
  - `nasa_api`: HTTP connection; `extra` must include `api_key`.
  - `my_postgres_connection`: Postgres connection pointing to the local containerized database.
- Containers and runtime:
  - `Dockerfile`: based on `astrocrpublic.azurecr.io/runtime:3.1-8`, providing the Airflow runtime.
  - `docker_compose.yml`: starts Postgres (ports 5002:5002, data volume `postgres_data`) on network `airflow_network`, shared with Airflow.

## DAG Tasks
- `create_table` (@task): uses `PostgresHook` to create table `apod_data` (title, explanation, url, date, media_type).
- `extract_apod` (HttpOperator): calls the `planetary/apod` endpoint via the `nasa_api` connection to fetch daily JSON.
- `transform_apod_data` (@task): selects needed fields and standardizes keys, returning a dict.
- `load_data_to_postgres` (@task): inserts the transformed data into `apod_data`.

## Local Run
1) Prereqs: install Docker and Astronomer CLI (or another Airflow container runtime).
2) Start Postgres dependency:
   ```bash
   docker compose up -d postgres
   ```
3) Start Airflow from repo root (Astronomer CLI example):
   ```bash
   astro dev start
   ```
   The Airflow containers automatically mount `dags/`, `plugins/`, and `requirements.txt`.
4) Configure connections in the Airflow UI:
   - `nasa_api`: `Conn Type=HTTP`, `Host=https://api.nasa.gov`, add `{"api_key": "<your-key>"}` in Extra.
   - `my_postgres_connection`: host `postgres`, port `5002` (matches compose), user/password `postgres`, schema `postgres`.
5) Enable the DAG `NASA_apod_postgres` in the Airflow UI to schedule daily or trigger manually.

## Directory Quick Reference
- `dags/etl.py`: DAG definition and dependencies.
- `docker_compose.yml`: local Postgres service.
- `Dockerfile`: Airflow runtime base image.
- `requirements.txt`: extra Airflow providers (HTTP, Postgres).
- `tests/dags/`: sample DAG-level tests.

## Notes
- The example focuses on task orchestration and external service integration; add data validation and retry policies as needed.
- To run without Astronomer, supply your own Airflow docker-compose and mount `dags/` and `requirements.txt` into scheduler/webserver containers.

