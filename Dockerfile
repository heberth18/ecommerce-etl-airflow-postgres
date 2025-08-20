ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"
