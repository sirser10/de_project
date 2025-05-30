from pathlib import Path
from datetime import datetime as dttm
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow import settings as airflow_settings

from operators.sap_api_operator import SapAPIOperator
from operators.common.sql_script_executor import SQLScriptExecution
from configs.sap_api_config import EMPLOYEE_SAP_CFG, LEGAL_STRUCT_RENAME_COLUMN
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

default_args = globals.DEFAULT_DAG_CONFIG.copy()
default_args['default_args']['owner'] = 's.frolkin'

SAP_ENTITIES_CFG =\
    {
        'org_structure' :{
                            'endpoint':'rest_api/dwh_analytics/',
                            'params': {
                                    'entity_param': Variable.get('getsap_org_url_param'),
                            },
                            'json': {**EMPLOYEE_SAP_CFG},
                            'method':'post',
                            'auth':True,
        },
        'bu_structure':{
                        'endpoint':'rest_api/dwh_analytics/',
                        'params': {
                            'entity_param':Variable.get('getsap_bu_url_param')
                        },
                        'method':'post',
                        'auth':True,
        },
        'jira_org_structure' :{
                                'endpoint':'rest_api/jira/',
                                'params': {
                                            'entity_param': 'get_org_struc',
                                },
                                'json': {**EMPLOYEE_SAP_CFG},
                                'method':'post',
                                'auth':True,
        }
}
LEGAL_ENTITY_CFG ={
        'endpoint':Variable.get('sap_employee_endpoint'),
        'params': {
            'entity_param': "(Pernr='{}',",
            'date_param':"Date={})"
        },
        'method':'get',
        'auth':True,
        'columns_to_rename': LEGAL_STRUCT_RENAME_COLUMN
}

with DAG(
        **globals.DEFAULT_DAG_CONFIG,
        dag_id           = DAG_ID,
        description      = 'Load applicants data from SAP to DWH and process it',
        schedule_interval = '45 5 * * *',
        start_date       = dttm(2024, 6, 3),
        tags             = ['sap'],
        concurrency      = 2
) as dag:
    

    path_to_sql_folder: str = airflow_settings.DAGS_FOLDER + '/sql/sap/'
    start_dag        = DummyOperator(task_id='start_dag')
    start_etl        = DummyOperator(task_id='start_etl')
    end_etl          = DummyOperator(task_id='end_etl')
    finish_dag       = DummyOperator(task_id='finish_dag')
    
    source_tasks = {}
    for k, v in SAP_ENTITIES_CFG.items():
        SOURCE_TASK_CFG = {
                            'task_id':k,
                            'task_key':'fetch_data_from_source',
                            'conn_id': Variable.get('sap_conn_id'),
                            'sap_config': v,
                            'legal_sap_cfg': LEGAL_ENTITY_CFG,
                            'pg_cfg': {
                                'pg_hook_con': ENV,
                                'pg_src_info_cfg': {
                                        'select_column' : ['table_name','column_name'],
                                        'info_table': 'columns'
                                    },
                                'prefix':'sap'
                            }
        }
        source_task = SapAPIOperator(**SOURCE_TASK_CFG)
        source_tasks[k] = source_task
        start_dag >> start_etl >> source_task

    task_mapping = {
                    'org_structure': [
                                    'sap_dim_positions',
                                    'sap_dim_org_units',
                                    'sap_dim_employees',
                                    'sap_dim_legal_struct'
                    ],
                    'bu_structure': ['sap_dim_bu'],
                    'jira_org_structure': [
                                    'sap_dim_positions',
                                    'sap_dim_org_units',
                                    'sap_dim_employees',
                    ],
    }
    """
    STG-TASK SEQUENCE
    """
    stg_tasks = {}
    for task_list in task_mapping.values():
        for table in task_list:
            if table not in stg_tasks:
                STG_TASK_CFG = {
                    'task_id': f'stg.{table}',
                    'pg_hook_con': ENV,
                    'read_sql_cfg': {
                        'directory': path_to_sql_folder,
                        'schema_folder': 'stg',
                        'table_name': table
                    }
                }
                stg_tasks[table] = SQLScriptExecution(**STG_TASK_CFG)

    for src_key, tables in task_mapping.items():
        src_task = source_tasks[src_key]
        for table in tables:
            stg_task = stg_tasks[table]
            src_task >> stg_task

    """
    ODS-TASK SEQUENCE
    """
    ods_tasks = {}
    for table in stg_tasks.keys():
        ODS_TASK_CFG = {
            'task_id': f'ods.{table}',
            'pg_hook_con': ENV,
            'read_sql_cfg': {
                'directory': path_to_sql_folder,
                'schema_folder': 'ods',
                'table_name': table
            }
        }
        ods_task = SQLScriptExecution(**ODS_TASK_CFG)
        ods_tasks[table] = ods_task

    for table, stg_task in stg_tasks.items():
        ods_task = ods_tasks[table]
        stg_task >> ods_task
    
    """
    DDS-TASK SEQUENCE
    """
    dds_tasks={}
    for table in ods_tasks.keys():
        DDS_TASK_CFG = {
                'task_id': 'dds.{}_history'.format(table),
                'pg_hook_con': ENV,
                'read_sql_cfg': {
                    'directory': path_to_sql_folder,
                    'schema_folder': 'dds',
                    'table_name': table + '_history'
                }
        }
        dds_task = SQLScriptExecution(**DDS_TASK_CFG)
        dds_tasks[table] = dds_task
    
    for table, ods_task in ods_tasks.items():
        dds_task = dds_tasks[table]
        ods_task >> dds_task >> end_etl >> finish_dag