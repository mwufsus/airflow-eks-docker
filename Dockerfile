FROM apache/airflow:2.1.2-python3.7

LABEL version="1.0.0"

RUN pip install --user pytest
RUN pip install --user airflow-dbt
RUN pip install --user dbt

COPY dags/ ${AIRFLOW_HOME}/dags
COPY config/ ${AIRFLOW_HOME}/config
COPY creditgenie_airflow/ ${AIRFLOW_HOME}/creditgenie_airflow
COPY data-warehouse/ ${AIRFLOW_HOME}/data-warehouse
COPY dbt/ ${AIRFLOW_HOME}/dbt
COPY unittests.cfg ${AIRFLOW_HOME}/unittests.cfg
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY unittests/ ${AIRFLOW_HOME}/unittests
COPY integrationtests ${AIRFLOW_HOME}/integrationtests