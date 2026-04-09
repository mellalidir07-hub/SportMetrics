# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 10:35:37 2026

@author: fafap
"""


import requests, logging
from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator


def execute_n8n_workflow(): 
    url = "https://automation.lacapsule.academy/webhook/50e27a5c-e779-4565-8d17-0255c367ea4c" 
    response = requests.get(url) 
    response.raise_for_status() 
    logging.info(response.json()) 
 
    return response.json() 

default_args = { 
    'owner': 'fabrice.pompui', 
    'depends_on_past': False, 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
} 
 
with DAG( 
    'user_fabrice_pompui_pipeline_sports_metrics', 
    default_args=default_args, 
    schedule_interval=timedelta(days=1), 
    start_date=datetime(2026, 3, 4), 
    catchup=False, 
) as dag: 
    
    execute_n8n_workflow = PythonOperator( 
        task_id="execute_n8n_workflow", 
        python_callable=execute_n8n_workflow 
    ) 
    
    dbt_run = DbtCloudRunJobOperator(
        task_id = "dbt_run",
        dbt_cloud_conn_id = "sport_metrics_dbt",
        account_id = 70471823539460,
        job_id = 70471823570301,
    )
    
    execute_n8n_workflow >> dbt_run