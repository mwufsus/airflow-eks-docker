from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from creditgenie_airflow.utils.redshift_download_operator import PgDownloadOperator

date_string = datetime.now().date()

formatted_sql = """select est_converted_date as application_start_date,
                        borrower_id,
                        email,
                        pricing_tier,
                        environmentname,
                        router,
                        completed_application,
                        utmsource,
                        decision_generated,
                        dmp_enrolled
                        from public.daily_page_conversions
                        where daily_page_conversions.est_converted_date < date_trunc('month', current_date) and
                        daily_page_conversions.est_converted_date >= date_trunc('month', current_date) - interval '1 month';
"""

default_args = {
    "owner": "Kyle & Matt",
    "start_date": datetime(2021, 9, 1)
}

dag = DAG(
    "nfcc_billing",
    description="Monthly Counselor Genie Billing",
    schedule_interval="5 1 1 * *",
    default_args=default_args,
    catchup=False,
    template_searchpath = ['/usr/local/airflow/']
)

nfcc_billing_task = PgDownloadOperator(
    task_id='nfcc_billing_task',
    postgres_conn_id='redshift_default',
    sql = formatted_sql,
    # for downloading large table
    pandas_sql_params={
        "chunksize": 100,
    },
    csv_path="/usr/local/airflow/{{ task.task_id }}_{{ ds }}.csv",
    csv_params={
        "sep": ",",
        "index": False,
    },
    depends_on_past=True,
    dag=dag,
)

task_id_str = str(nfcc_billing_task.task_id)
file_name = str(nfcc_billing_task.csv_params)

email_task = EmailOperator(
        task_id='send_email',
        to=['clynton.kakai@creditgenie.com'
            ,'kyle.good@creditgenie.com'
            ,'matt.wufsus@creditgenie.com'],
        subject='Airflow Alert',
        html_content=""" Billing Job for Previous Month """,
        files=[f"/usr/local/airflow/{task_id_str}_" + f"{date_string}" + ".csv"],
        dag=dag
)

nfcc_billing_task >> email_task
