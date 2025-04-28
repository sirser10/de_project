from pathlib import Path 
from datetime import datetime as dttm


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import  BranchPythonOperator
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from manatal_project.operators.file_loader_operator import CustomFilesOperator, FileHashDetector
from manatal_project.config.manual_table_config import RENAME_COLUMNS, RENAME_FILE
from manatal_project.operators.sql_script_executor import SQLScriptExecution


import logging
import warnings
import os

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

DAG_ID            = Path(__file__).name.rsplit(".", 1)[0]
AIRFLOW_HOME: str = os.getenv('AIRFLOW_HOME')
ENV: str          = os.environ.get('ENV')
sql_path: str     = AIRFLOW_HOME + '/dags/manatal_project/sql/manatal/'

if ENV == 'PROD':
    log.info(f'Start working in PROD environment')
else:
    log.info(f'Start working in DEV environment')

def decide_next_task(input_task_id: str, next_task_name: str,**kwargs) -> str:
    """DO or NOT start next airflow task"""
    changes_detected = kwargs['ti'].xcom_pull(task_ids=input_task_id, key='changes_detected')
    return next_task_name if changes_detected else 'end_dag' 


with DAG(
        dag_id=DAG_ID,
        description='Loader Manatal recruitment data to DWH',
        schedule_interval='*/30 * * * *',
        start_date=dttm(2025, 4, 28),
        tags=['manual files', 'recruitment'],
        catchup=False
        ):
    
    start_dag     = DummyOperator(task_id='start_dag')
    end_dag       = DummyOperator(task_id='end_dag')
    start_dwh_etl = DummyOperator(task_id='start_dwh_etl')
    end_dwh_etl   = DummyOperator(task_id='end_dwh_etl')

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
                                                    'erase_method': 'drop',
                                                    'rename_col_mapping': RENAME_COLUMNS,
                                                    'file_name_mapping':RENAME_FILE
                                        },
                    }
    }
    filehash_check_task = FileHashDetector(**LOADING_CFG['file_hash_detector_params'])
    decide_task         = BranchPythonOperator(**LOADING_CFG['decide_next_task'])
    file_loader_task    = CustomFilesOperator(**LOADING_CFG['file_loader_params'])


    start_dag >> filehash_check_task >> decide_task >> file_loader_task >> start_dwh_etl

    with TaskGroup(group_id='stg_proceessor') as stg_processor:
        stg_tables = [tab.split('/')[-1].replace('.sql', '') for tab in os.listdir(os.path.join(sql_path, 'stg'))]

        for t in stg_tables:
            stg_cfg = {
                        'pg_hook_con': ENV,
                        'task_id':'stg.'+t,
                        'read_sql_cfg':{
                                        'directory': sql_path,
                                        'schema_folder': 'stg',
                                        'table_name': t
                        }
            }
            stg_task = SQLScriptExecution(**stg_cfg)
    
    with TaskGroup(group_id='ods_processor') as ods_processor:
        ods_tables = [tab.split('/')[-1].replace('.sql', '') for tab in os.listdir(os.path.join(sql_path, 'ods')) ]

        for t in ods_tables:
            ods_cfg = {
                        'pg_hook_con': ENV,
                        'task_id':'ods.'+t,
                        'read_sql_cfg':{
                                        'directory': sql_path,
                                        'schema_folder': 'ods',
                                        'table_name': t
                        }
            }
            ods_task = SQLScriptExecution(**ods_cfg)

        
    with TaskGroup(group_id='dds_processor') as dds_processor:
        dds_tables = [tab.split('/')[-1].replace('.sql', '') for tab in os.listdir(os.path.join(sql_path, 'dds'))]

        for t in dds_tables:
            dds_cfg = {
                        'pg_hook_con': ENV,
                        'task_id':'dds.'+t,
                        'read_sql_cfg':{
                                        'directory': sql_path,
                                        'schema_folder': 'dds',
                                        'table_name': t
                        }
            }
            dds_task = SQLScriptExecution(**dds_cfg)

    start_dwh_etl >> stg_processor  >> ods_processor >> dds_processor >>  end_dwh_etl >> end_dag