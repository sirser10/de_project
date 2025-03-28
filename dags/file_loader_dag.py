from pathlib import Path 
from datetime import datetime as dttm

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import  BranchPythonOperator
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from operators.common.file_loader_operator import CustomFilesOperator, FileHashDetector
from operators.common.pg_operator import CustomPGOperator
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

    hran_mnt_dir: str            = Variable.get('hran_mnt_dir')
    manual_files_dir: str        = hran_mnt_dir + '/manual_files/' + 'manual_files_dwh/'
    manual_file_hashes_path: str = hran_mnt_dir + '/manual_files/' + 'manual_file_hash_logs/' + 'manual_file_hashes.txt'

    LOADING_CFG =\
                {
                    'file_hash_detector_params':{
                        'task_id': 'filehash_change_check',
                        'task_key': 'execute_filehash_change',
                        'dir_path' : manual_files_dir,
                        'hash_file_path': manual_file_hashes_path,
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
                                        'dir_path' : manual_files_dir,
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

    with TaskGroup(group_id='stg_processor') as stg_processor:
        for k, v in MANUAL_TABLE_STG_CFG.items():
            previous_task = None
            STG_CFG =\
                {
                    'task_id': 'stg.' + k,
                    'task_key':'stg_processor',
                    'pg_hook_con': ENV,
                    'stg_cfg': {
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
            stg_task = CustomPGOperator(**STG_CFG)
            if previous_task:
                previous_task >> stg_task 
            previous_task = stg_task
            
        start_dwh_etl >> stg_processor

    with TaskGroup(group_id='ods_processor') as ods_processor:
        for k, v in ODS_TABLES_CFG.items():
            previous_task=None
            ODS_CFG =\
                {
                    'task_id': 'ods.' + k,
                    'task_key':'ods_processor',
                    'pg_hook_con':ENV,
                    'ods_cfg': {
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
            ods_task = CustomPGOperator(**ODS_CFG)
            if previous_task:
                previous_task >> ods_task 
            previous_task = ods_task
        
        stg_processor >> ods_processor
    
    ods_processor >> end_dwh_etl >> end_dag