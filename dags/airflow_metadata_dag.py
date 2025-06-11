from pathlib import Path 
from datetime import datetime as dttm

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow import settings as airflow_settings

from operators.airflow_metadata_operator import AirflowMetadataOperator
from operators.common.sql_script_executor import SQLScriptExecution
from globals import globals

import os
import logging

log        = logging.getLogger(__name__)
DAG_ID     = Path(__file__).name.rsplit(".", 1)[0]
DAGS_PATH  = airflow_settings.DAGS_FOLDER
ENV        = os.environ.get('ENV')

if ENV == 'PROD':
    log.info(f'Start working in PROD environment')
else:
    log.info(f'Start working in DEV environment')

default_args = globals.DEFAULT_DAG_CONFIG.copy()
default_args['default_args']['owner'] = 's.frolkin'

with DAG(
            **globals.DEFAULT_DAG_CONFIG,
            dag_id = DAG_ID,
            description ='Loader of all Airflow DAGS and Tasks metadata',
            schedule_interval = '@once',
            start_date= dttm(2024, 12, 5),
            tags = ['airflow', 'metadata']
):
    
    start_dag=DummyOperator(task_id='start_dag')
    end_dag=DummyOperator(task_id='end_dag')
    load_airflow_tasks_cfg = {

        'load_to_pg_task': {

            'task_id': 'load_airflow_tasks_to_db',
            'task_key':'load_airflow_tasks_to_db',
            'pg_hook_con':ENV,
        },
        'pgtable_processing_task': {

            'task_id': 'metadata.airflow_task_pgtable_entities',
            'task_key':'sql_script_execution',
            'pg_hook_con':ENV,
            'read_sql_cfg': {
                'directory': DAGS_PATH + '/sql/metadata/',
                'table_name':'metadata.airflow_task_pgtable_entities'
            }
        }
    }

    load_to_db_task = AirflowMetadataOperator(**load_airflow_tasks_cfg['load_to_pg_task'])
    task_pgtable_entities_task = SQLScriptExecution(**load_airflow_tasks_cfg['pgtable_processing_task'])


    start_dag >> load_to_db_task >> task_pgtable_entities_task >> end_dag

    

