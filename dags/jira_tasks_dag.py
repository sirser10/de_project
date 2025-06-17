from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable


from operators.jira_tasks_operator import JiraTasksOperator, JiraOdsProcessor
from operators.common.sql_script_executor import SQLScriptExecution
from configs.jira_tasks_config import (
                                                RESPONSE_ISSUE_FIELDS,
                                                API_CONFIG,
                                                CREDS,
                                                FIELD_NAME_MAPPING
)
from datetime import datetime as dttm
from pathlib import Path
from globals import globals

import logging
import os


log = logging.getLogger(__name__)
DAG_ID = Path(__file__).name.rsplit(".", 1)[0]

ENV = os.environ.get('ENV')
if ENV == 'PROD':
    log.info(f'Start working in PROD environment')
else:
    log.info(f'Start working in DEV environment')


with DAG(
    **globals.DEFAULT_DAG_CONFIG,
    dag_id=DAG_ID,
    start_date=dttm(2024, 10, 16),
    schedule_interval='30 2 * * *',
    description='Ingestion and processing JIRA tasks',
    tags=['jira']

) as dag:
    
    sql_folder =  os.getenv('AIRFLOW_HOME') + '/dags/sql/jira_tasks/'
    
    start_dag        = DummyOperator(task_id='start_dag')
    end_dag          = DummyOperator(task_id='end_dag')

    api_token: str =Variable.get('token')
    creds = CREDS.copy()
    creds['token'] = creds['token'] % api_token

    PG_CFG=\
    {
        'pg_ingest_cfg': {
                        'pg_hook_con': ENV,
                        'schema': 'src',
                        'prefix': 'jira',
                        'table_name': 'tasks',
        },
        'pg_stg_cfg':{
                    'directory': sql_folder,
                    'schema_folder' : 'stg',
                    'table_name': 'jira_tasks'
        }
    }

    task_cfg=\
    {
        'ingest_cfg': {
                        'task_id': 'ingest_data_from_source_to_db',
                        'task_key': 'extract_jira_to_db',
                        'conn_id': 'jira_id',
                        'api_extraction_params': {
                            'api_cfg': API_CONFIG,
                            'response_issue_fields': RESPONSE_ISSUE_FIELDS,
                            'creds': creds,
                        },
                        'pg_cfg':{**(PG_CFG['pg_ingest_cfg'])}
        },
        'stg_cfg': {
            'task_id':'stg_jira_tasks',
            'pg_hook_con': ENV,
            'read_sql_cfg': PG_CFG['pg_stg_cfg']
        },
        'ods_cfg': {
            'task_id': 'ods_jira_tasks',
            'task_key': 'jira_ods_processor',
            'pg_hook_con': ENV,
            'ods_cfg': {
                    'schema_from':'stg',
                    'schema_to':'ods',
                    'table_name':'jira_tasks',
                    'jsonb_field': 'fields',
                    'rename_config': FIELD_NAME_MAPPING
            }
        }
    }
    ingest_data_from_source_to_db_task = JiraTasksOperator(**task_cfg['ingest_cfg'])
    stg_tng_study_task                 = SQLScriptExecution(**task_cfg['stg_cfg'])
    ods_tng_study_task                 = JiraOdsProcessor(**task_cfg['ods_cfg'])


    start_dag >> ingest_data_from_source_to_db_task >> stg_tng_study_task >> ods_tng_study_task >> end_dag



