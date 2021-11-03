FROM apache/airflow:2.1.2-python3.7

LABEL version="1.0.0"

RUN pip install --user pytest
    && pip install 'dbt==0.21.0' \
    && pip install 'airflow-dbt==0.4.0' 

COPY dags/ ${AIRFLOW_HOME}/dags
COPY unittests.cfg ${AIRFLOW_HOME}/unittests.cfg
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY unittests/ ${AIRFLOW_HOME}/unittests
COPY integrationtests ${AIRFLOW_HOME}/integrationtests