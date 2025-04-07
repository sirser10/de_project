from datetime import datetime as dttm
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from de_project.dags.configs.sap_oracle_config import (
                                    RENAME_COLUMNS,
                                    FILE_NAME_MAPPING,
                                    PRIMARY_KEYS_CONFIG,
                                    ODS_TABLES_CFG
                                )
from operators.sap_oracle_operator import SapOracleOperator, SapOracleOdsOperator
from operators.common.s3_operator import S3CustomOperator
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

with DAG(
        **globals.DEFAULT_DAG_CONFIG,
        dag_id=DAG_ID,
        description= 'Load goals data from SAP to DWH and process it',
        start_date = dttm(2024, 8, 5),
        schedule_interval = '30 6 * * *',
        tags=['sap'],
        concurrency = 2
        ) as dag:

    bucket_name: str = 'bucker_name'
    ingest_path: str = f'ingest/sap_oracle/'
    stage_path: str = f'stage/sap_oracle/'
    distilled_path: str = f'distilled/sap_oracle/'

    start_dag        = DummyOperator(task_id='start_dag')
    end_dag          = DummyOperator(task_id='end_dag')
    start_dwh_etl    = DummyOperator(task_id='start_dwh_etl')
    end_dwh_etl      = DummyOperator(task_id='end_dwh_etl')

    S3_INIT_KWARGS = {
                        's3_hook_con': ENV,
                        'bucket': bucket_name,
    }
    
    SOURCE_CFG = {
        'task_id':'get_data_from_oracle_to_s3',
        'task_key': 'get_data_from_source',
        'oracle_cfg': {
                        'oracle_conn_id': ENV,
                        'source_schema': Variable.get('oracle_prod_schema'),
                        'user_available_list_t_tab': 'user_tab_privs',                        
        },
        's3_cfg':{
                  **S3_INIT_KWARGS,
                  's3_ingest_path': ingest_path
        }
    }
    
    ingest_task = SapOracleOperator(**SOURCE_CFG)

    start_dag >> ingest_task

    with TaskGroup(group_id='s3_processor') as s3_processor:
        S3_PROCESSOR_TASK_CFG = {
            'stage': {
                **S3_INIT_KWARGS,
                'task_id': 's3_stage_processor',
                'task_key': 's3_stage_processor',
                'stage_cfg':{
                            'source_path': ingest_path,
                            'dest_path': stage_path,
                            'rename_col_mapping': RENAME_COLUMNS,
                            'file_name_mapping':FILE_NAME_MAPPING,
                            'write_format':'csv',
                            'encoding_detector': False
                }
            },
            'distilled' : {
                **S3_INIT_KWARGS,
                'task_id': 's3_distilled_processor',
                'task_key': 's3_distilled_processor',
                'distilled_cfg':{
                                'source_path': stage_path,
                                'dest_path': distilled_path,
                                'distillation_proc_cfg': PRIMARY_KEYS_CONFIG,
                                'write_format': 'csv',

                },
            },
            'dwh_ingestion': {
                            **S3_INIT_KWARGS,
                            'task_id': 's3_loading_to_db_processor',
                            'task_key': 's3_loading_to_db_processor',
                            'pg_cfg': {
                                    'pg_hook_con': ENV,
                                    'source_path': distilled_path,
                                    'schema': 'stg',
                                    'prefix':'sapbw',
                                    'read_format':'csv',
                                    'erase_method':'drop',
                                    'pg_src_info_cfg': {
                                        'select_column' : ['table_name','column_name'],
                                        'info_table': 'columns'
                                    },
                                    'void_replace': True,
                            }
            }
        }

        previous_task = None
        for stage, params in S3_PROCESSOR_TASK_CFG.items():
            s3_task = S3CustomOperator(**params)
            if previous_task:
                previous_task >> s3_task
            previous_task = s3_task
        
    ingest_task >> s3_processor >> start_dwh_etl

    with TaskGroup(group_id='ods_processor') as ods:
        for k, v in ODS_TABLES_CFG.items():
            ods_task_cfg={
                'task_id':'{}.{}_{}'.format('ods', 'sapbw',k),
                'task_key':'sapbw_ods_processor',
                'pg_hook_con':ENV,
                'ods_cfg':{
                            'schema_from':'stg',
                            'schema_to':'ods',
                            'prefix':'sapbw',
                            'table_name': k,
                            'column_list':v['columns'],
                            'columns_to_deduplicate': v['columns_to_deduplicate'],
                            'info_tab_conf': {
                                            'select_column' : ['table_name','column_name'],
                                            'info_table': 'columns'
                                            },
                }
            }
            ods_task = SapOracleOdsOperator(**ods_task_cfg)
    
    start_dwh_etl >> ods >> end_dwh_etl >> end_dag
