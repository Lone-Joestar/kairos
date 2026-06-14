FROM apache/airflow:2.9.1-python3.11

#switch to root
USER root

RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# switch back to airflow user 
USER airflow

#python dependencies

COPY requirements/airflow.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt