from pathlib import Path 
from datetime import datetime as dttm

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from operators.common.file_loader_operator import CustomFilesOperator, FileHashDetector
from operators.common.pg_operator_one_table import stg_processor, ods_upsert
from configs.manual_table_config import MANUAL_TABLE_STG_CFG, ODS_TABLES_CFG
from globals import globals

import logging
import warnings
import os

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

DAG_ID            = Path(__file__).name.rsplit(".", 1)[0]
AIRFLOW_HOME: str = os.getenv('AIRFLOW_HOME')
ENV: str          = os.environ.get('ENV')

if ENV == 'PROD':
    log.info(f'Start working in PROD environment')
else:
    log.info(f'Start working in DEV environment')

def decide_next_task(input_task_id: str, next_task_name: str,**kwargs) -> str:
    """DO or NOT start next airflow task"""
    changes_detected = kwargs['ti'].xcom_pull(task_ids=input_task_id, key='changes_detected')
    return next_task_name if changes_detected else 'end_dag' 

def create_dwh_task(task_id, python_callable, **kwargs) -> PythonOperator:
    return PythonOperator(
                            task_id=task_id,
                            python_callable=python_callable,
                            op_kwargs=kwargs,
                            provide_context=True
                            )

default_args = globals.DEFAULT_DAG_CONFIG.copy()
default_args['default_args']['owner'] = 's.frolkin'

with DAG(
        **globals.DEFAULT_DAG_CONFIG,
        dag_id=DAG_ID,
        description='Loader of CSV files from HRAN share folder',
        schedule_interval='*/30 * * * *',
        start_date=dttm(2024, 12, 23),
        tags=['sharefolder','manual files'],
        ):

    start_dag  = DummyOperator(task_id='start_dag')
    end_dag    = DummyOperator(task_id='end_dag')
    start_dwh_etl = DummyOperator(task_id='start_dwh_etl')
    end_dwh_etl = DummyOperator(task_id='end_dwh_etl')

    LOADING_CFG =\
                {
                    'file_hash_detector_params':{
                        'task_id': 'filehash_change_check',
                        'task_key': 'execute_filehash_change',
                        'dir_path' : AIRFLOW_HOME + Variable.get('sharefile_dir_path'),
                        'hash_file_path': AIRFLOW_HOME + Variable.get('filehash_logs_path') + 'hran_file_hashes.txt'
                    },
                    'decide_next_task': {
                                        'task_id':'decide_next_task',
                                        'python_callable': decide_next_task,
                                        'op_args':[
                                                'filehash_change_check',
                                                'loading_file_to_dwh'
                                        ],
                                        'provide_context':True
                    },
                    'file_loader_params': {
                                        'task_id':'loading_file_to_dwh',
                                        'task_key':'find_and_load_file_to_db',
                                        'dir_path' : AIRFLOW_HOME + Variable.get('sharefile_dir_path'),
                                        'pg_src_cfg': {
                                                    'pg_hook_con': ENV, 
                                                    'pg_src_info_cfg':{'select_column': '1', 'info_table': 'columns'},
                                                    'prefix':'manual',
                                                    'erase_method': 'drop'
                                        },
                    }
    }
    filehash_check_task = FileHashDetector(**LOADING_CFG['file_hash_detector_params'])
    decide_task         = BranchPythonOperator(**LOADING_CFG['decide_next_task'])
    file_loader_task    = CustomFilesOperator(**LOADING_CFG['file_loader_params'])

    start_dag >> filehash_check_task >> decide_task >> file_loader_task >> start_dwh_etl

    with TaskGroup(group_id='stg_processor') as stg_processor_:
        for k, v in MANUAL_TABLE_STG_CFG.items():
            previous_task = None
            STG_PROCESSOR_CFG =\
                            {
                                'python_callable':stg_processor,
                                'op_kwargs': {
                                            'pg_hook_con':ENV,
                                            'schema_from':'src',
                                            'schema_to':'stg',
                                            'table_name': k,
                                            'column_list': v['columns'],
                                            'columns_to_deduplicate': v['columns_to_deduplicate'],
                                            'info_tab_conf': {
                                                            'select_column' : ['table_name','column_name'],
                                                            'info_table': 'columns'
                                            } 
                                }
                            }
            task=create_dwh_task(
                                task_id='stg.'+ k,
                                python_callable=STG_PROCESSOR_CFG['python_callable'],
                                **STG_PROCESSOR_CFG['op_kwargs']
                                )
            if previous_task:
                previous_task >> task 
            previous_task = task
            
        start_dwh_etl >> stg_processor_

    with TaskGroup(group_id='ods_processor') as ods_processor:
        for k, v in ODS_TABLES_CFG.items():
            previous_task=None

            ODS_PROCESSOR_CFG =\
                {
                    'python_callable':ods_upsert,
                    'op_kwargs': {
                                'pg_hook_con':ENV,
                                'schema_from':'stg',
                                'schema_to':'ods',
                                'table_name': k,
                                'column_list': v['columns'],
                                'unique_constraints': v['unique_constraints'],
                                'columns_to_upsert': v['columns_to_upsert'],
                                'info_tab_conf': {
                                                'select_column' : ['table_name','column_name'],
                                                'info_table': 'columns'
                                } 
                    }
                }
            task=create_dwh_task(
                                task_id='ods.'+ k,
                                python_callable=ODS_PROCESSOR_CFG['python_callable'],
                                **ODS_PROCESSOR_CFG['op_kwargs']
                                )
            if previous_task:
                previous_task >> task 
            previous_task = task
        
        stg_processor_ >> ods_processor
    
    ods_processor >> end_dwh_etl >> end_dag

