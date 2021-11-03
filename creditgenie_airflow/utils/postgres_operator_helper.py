from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.base_hook import BaseHook

def create_copy_task(dag, task_name):
    return PostgresOperator(
        task_id = f"{task_name}_copy",
        name = f"{task_name}_copy",
        dag=dag
    )

def create_sensor_task(dag, task_name, bucket_key, bucket_name, connection, fail_method, interval, mode, wildcard_match):
    return S3KeySensor(
    dag=dag,
    task_id = f"{task_name}_sensor",
    name = f"{task_name}_sensor",
    bucket_key=bucket_key,
    bucket_name=bucket_name,
    aws_conn_id=connection,
    soft_fail=fail_method,
    poke_interval=interval,
    mode=mode,
    wildcard_match=wildcard_match
)

def create_alpha_task(dag, task_name):
    return PostgresOperator(
        task_id = f"{task_name}_copy",
        name = f"{task_name}_copy",
        dag=dag
    )

