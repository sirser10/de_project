from pathlib import Path 
from datetime import datetime as dttm

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from operators.common.datahub_operator import CustomDatahubOperator, CustomDatahubLineage

from globals import globals

import os
import logging
import yaml

log = logging.getLogger(__name__)
DAG_ID = Path(__file__).name.rsplit(".", 1)[0]

ENV = os.environ.get('ENV')
if ENV == 'PROD':
    log.info(f'Start working in PROD environment')
else:
    log.info(f'Start working in DEV environment')


datahub_cfg ={
    'url': Variable.get('datahub_host'),
    'datahub_token': Variable.get('datahub_password'),
    'domain_name':'any',
    'platform_name':'postgres',
    'db_name':'any'
}

default_args = globals.DEFAULT_DAG_CONFIG.copy()
default_args['default_args']['owner'] = 's.frolkin'

with DAG(
        **globals.DEFAULT_DAG_CONFIG,
        dag_id           = DAG_ID,
        description      = 'Adding new entities to DataHub and building lineage between DWH entities',
        schedule_interval='@once',
        start_date       = dttm(2024, 12, 6),
        tags             = ['datahub', 'lineage']
):
    
    start_dag=DummyOperator(task_id='start_dag')
    end_dag=DummyOperator(task_id='end_dag')

    DATAHUB_YML_PATH: str = os.getenv('AIRFLOW_HOME') + '/dags/datahub/'
    ALL_ATTRS_CFG, TABLE_ATTRS_CFG, LINEAGE_DEPENDENCIES_CFG  = {}, {}, {}

    for path, dir, yaml_ in os.walk(DATAHUB_YML_PATH):
        yaml_files = [y for y in yaml_ if y != '__init__.py']
        for name in yaml_files:
            file_path = os.path.join(path,name)
            content: dict = yaml.safe_load(open(file_path, 'r'))
            ALL_ATTRS_CFG.update(content)

    for table, params in ALL_ATTRS_CFG.items():
        description_cfg = params['description_cfg']
        lineage = params.get('lineage', {})
        TABLE_ATTRS_CFG[table] = {k: v for k, v in description_cfg.items()}
        if lineage:
            if 'downstream' in lineage.keys():
                LINEAGE_DEPENDENCIES_CFG[table] = {k: v for k,v in lineage.items()}
            else:
                LINEAGE_DEPENDENCIES_CFG[table] = list(lineage.values())[0]
    
    with TaskGroup(group_id='ingest_group') as ingest_group:

        for k, v in TABLE_ATTRS_CFG.items():

            ingest_task = CustomDatahubOperator(
                                            task_id=k,
                                            task_key='get_metadata_from_postgres',
                                            pg_conn_id=ENV,
                                            datahub_conn_cfg=datahub_cfg,
                                            data_catalog_cfg={
                                                            'table_description': v.get('table_description'),
                                                            'field_description_cfg':v['field_description'],
                                                            'column_tags': v.get('tag'),
                                                            'column_terms': v.get('glossary_terms'),
                                                            'target_schema': k.split('.')[0],
                                                            'target_table': k.split('.')[1]
                                            }
            )
            start_dag >> ingest_task

    with TaskGroup(group_id='entities_lineage') as entities_lineage:
        for t, val in LINEAGE_DEPENDENCIES_CFG.items():
            
            lineage_task = CustomDatahubLineage(
                                                task_id=f'{t}_lineage',
                                                task_key='process_lineage',
                                                datahub_conn_cfg=datahub_cfg,
                                                table_name=t,
                                                up_down_streams=val
                                            )

        ingest_group >> entities_lineage >> end_dag