from pathlib import Path 
from datetime import datetime as dttm
from typing import Literal
from configs.mytracker_config import (
                                        MYTRACKER_REPORT_PARAMS,
                                        CREDS,
                                        RENAME_COLUMS,
                                        DISTILLED_CONFIG,
                                        STG_CONFIG,
                                        ODS_TABLES
                                    )
from operators.mytracker_operator import MyTrackerOperator
from operators.common.s3_operator import S3CustomOperator
from operators.common.pg_operator import CustomPGOperator
from globals import globals

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.variable import Variable

import os
import logging

log = logging.getLogger(__name__)
DAG_ID = Path(__file__).name.rsplit(".", 1)[0]
LOAD_TYPE: Literal['full', 'incremental'] = 'incremental'

ENV = os.environ.get('ENV')
if ENV == 'PROD':
    log.info(f'Start working in PROD environment')
else:
    log.info(f'Start working in DEV environment')

default_args = globals.DEFAULT_DAG_CONFIG.copy()
default_args['default_args']['owner'] = 's.frolkin'

with DAG(
        **globals.DEFAULT_DAG_CONFIG,
        dag_id           = DAG_ID,
        description      = 'Load data from MyTracker to DWH and process it',
        schedule_interval= '30 4 * * *',
        start_date       = dttm(2024, 11, 14),
        tags             = ['mytracker'],
) as dag:
    
    secret_key: str =Variable.get('mytracker_api_secret_key')
    user_id: str = Variable.get('mytracker_api_user_id')

    creds = CREDS.copy()
    creds['api_user_id'] = creds['api_user_id'] %user_id
    creds['api_secret_key'] = creds['api_secret_key'] %secret_key

    bucket_name: str = 'hran'
    project_name: str = DAG_ID.split('_')[0]

    ingest_path: str     = 'ingest/{}/{}/{}/{}/'.format(project_name,dttm.today().year,dttm.today().month,dttm.today().day) if LOAD_TYPE == 'incremental' else 'ingest/' + project_name + '/full_load/'
    stage_path: str      = 'stage/' + project_name + '/parquet' + '/'
    distilled_path: str  = 'distilled/' + project_name + '/parquet' + '/'
    write_mode: str      = 'append' if LOAD_TYPE == 'incremental' else 'overwrite'


    FULL_ANNUAL_LOAD_PARAMS = {
        'increment_type':'annually',
        'start_year': 2024,
        'start_month': 1,
        }
    
    start_dag        = DummyOperator(task_id='start_dag')
    start_dwh_etl    = DummyOperator(task_id='start_dwh_etl')
    end_dwh_etl      = DummyOperator(task_id='end_dwh_etl')
    finish_dag       = DummyOperator(task_id='finish_dag')

    S3_INIT_KWARGS={
                    's3_hook_con':ENV,
                    'bucket': bucket_name,
            }
    with TaskGroup(group_id='extract_data_from_MyTracker') as source:
        ingest_methods_dct = {
                        'raw_data': 'get_mytracker_raw_data', 
                        'reports':'get_mytracker_reports'
                    }
        
        previous_task = None
        for group in MYTRACKER_REPORT_PARAMS.keys():

            mytacker_cfg = {
                            'task_id':'get_and_load_{}_into_S3'.format(group),
                            'task_key': ingest_methods_dct[group],
                            'pass_config':creds,
                            'report_params': MYTRACKER_REPORT_PARAMS[group],
                            's3_ingest_config': {
                                                **S3_INIT_KWARGS,
                                                'ingest_path': ingest_path
                            },
                            'load_type': LOAD_TYPE,
                            'date_edged_params': FULL_ANNUAL_LOAD_PARAMS if LOAD_TYPE == 'full' else {}
            }
            ingest_task = MyTrackerOperator(**mytacker_cfg)
            if previous_task:
                previous_task >> ingest_task
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
                                        'rename_col_mapping': RENAME_COLUMS,
                                        'write_format':'parquet',
                                        'write_mode': write_mode,
                            }
                        },
                        'distilled':{
                            **S3_INIT_KWARGS,
                            'task_id': 's3_distilled_processor',
                            'task_key': 's3_distilled_processor',
                            'distilled_cfg':{
                                'source_path': stage_path,
                                'dest_path': distilled_path,
                                'distillation_proc_cfg': DISTILLED_CONFIG,
                            },
                        },
                        'dwh_ingestion': {
                            **S3_INIT_KWARGS,
                            'task_id': 's3_loading_to_db_processor',
                            'task_key': 's3_loading_to_db_processor',
                            'pg_cfg': {
                                    'pg_hook_con': ENV,
                                    'source_path': distilled_path,
                                    'prefix':'mytracker',
                                    'read_format':'parquet',
                                    'erase_method':'drop',
                                    'pg_src_info_cfg': {
                                        'select_column' : ['table_name','column_name'],
                                        'info_table': 'columns'
                                    }
                            }
                        }
        }
        previous_task = None
        for k, v in S3_PROCESSOR_TASK_CFG.items():
            task = S3CustomOperator(**v)
            if previous_task:
                previous_task >> task
            previous_task = task
        
    source >> s3_processor

    with TaskGroup(group_id='stg_processor') as stg:
        for k, v in STG_CONFIG.items():
            stg_task_cfg_= {
                            'pg_hook_con': ENV,
                            'schema_from': 'src',
                            'schema_to': 'stg',
                            'prefix':'mytracker',
                            'table_name': k,
                            'column_list':v['columns'],
                            'columns_to_deduplicate':v['columns_to_deduplicate'],
                            'info_tab_conf': {
                                            'select_column' : ['table_name','column_name'],
                                            'info_table': 'columns'
                                            }
                            }
            stg_task = CustomPGOperator(
                                  task_id='{}.{}_{}'.format(stg_task_cfg_['schema_to'], stg_task_cfg_['prefix'], k),
                                  task_key='stg_processor',
                                  stg_cfg=stg_task_cfg_      
            )
    with TaskGroup(group_id='ods_processor') as ods:
            for k, v in ODS_TABLES.items():
                ods_task_cfg= {
                                'pg_hook_con':ENV,
                                'schema_from':'stg',
                                'schema_to':'ods',
                                'prefix':'mytracker',
                                'table_name': k,
                                'column_list':v['columns'],
                                'unique_constraints': v['unique_constraints'],
                                'columns_to_upsert':v['columns_to_upsert'],
                                'info_tab_conf': {
                                                'select_column' : ['table_name','column_name'],
                                                'info_table': 'columns'
                                                },
                                **({'is_drop_column': {'table': k, 'column': 'row_id'}} if k == 'dim_banner' else {})
                            }
                ods_task = CustomPGOperator(
                                  task_id='{}.{}_{}'.format(ods_task_cfg['schema_to'], ods_task_cfg['prefix'],k),
                                  task_key='ods_processor',
                                  stg_cfg=ods_task_cfg      
                )

    s3_processor>> start_dwh_etl >> stg >> ods >> end_dwh_etl >> finish_dag