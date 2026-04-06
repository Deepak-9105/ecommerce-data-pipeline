FROM apache/airflow:2.8.0-python3.11

USER root
# Install Java (Required for PySpark/Delta Lake)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Install Data Engineering libraries
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    apache-airflow-providers-amazon \
    apache-airflow-providers-snowflake