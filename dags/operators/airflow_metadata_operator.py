from typing import Tuple
from pandas import DataFrame

from airflow.models import DagBag
from airflow.models.baseoperator import BaseOperator
from helpers.common_helpers import CommonHelperOperator
from operators.common.pg_operator import CustomPGOperator

import logging
import warnings
import pandas as pd

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

class AirflowMetadataOperator(BaseOperator):

    def __init__(
                self,
                pg_hook_con: str,
                task_key: str=None,
                task_id: str=None,
                **kwargs
                )-> None:
        
        if task_id is not None:
            super().__init__(task_id=task_id,**kwargs)
        
        self.common_helper = CommonHelperOperator()
        self.pg_hook_con   = pg_hook_con
        self.task_key      = task_key if task_key is not None else task_id

        self.dag_context = {key: None for key in ['dag_id', 'dag_run_id', 'dag_start_dttm']}
        
        self.exec_method: dict = {
            'load_airflow_tasks_to_db':self.load_airflow_tasks_to_db
        }
        
    def execute(self, context):

        self.dag_context.update(
                                {
                                'dag_id': context['dag'].dag_id,
                                'dag_run_id': context['dag_run'].run_id,
                                'dag_start_dttm': context['dag_run'].execution_date.strftime('%Y-%m-%d %H:%M:%S')
                                }
                            )
        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res

    def fetch_all_dag_tasks(self) -> Tuple[DataFrame, dict]:

        dag_bag = DagBag()
        dag_task_dct={}

        for dag_id, dag in dag_bag.dags.items():
            dag_task_dct[dag_id] = [task.task_id for task in dag.tasks]
        log.info(f"DAG tasks: {dag_task_dct}")

        rows=[(dag_name, task) for dag_name, tasks in dag_task_dct.items() for task in tasks]
        metadata_columns: dict  = self.common_helper.get_metadata_columns(context=self.dag_context)
        df: DataFrame = pd.DataFrame(rows, columns=['dag_name', 'task']).assign(**metadata_columns)
        columns={col: 'TEXT' for col in df.columns}

        return df, columns

    def load_airflow_tasks_to_db(self)-> None:

        df_to_load, columns = self.fetch_all_dag_tasks()
        self.pg_operator    = CustomPGOperator(pg_hook_con=self.pg_hook_con)
        self.pg_operator.src_processor(
                                        source_obj=df_to_load,
                                        columns=columns,
                                        table_name='airflow_dag_tasks',
                                        pg_src_info_cfg= {
                                            'select_column':['table_name','column_name'],
                                            'info_table': 'columns'
                                        },
                                        method='drop'
                                        )
    






