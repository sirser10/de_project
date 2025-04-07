from typing import Any, List
from pandas import DataFrame
from hooks.oracle_hook import CustomOracleHook
from operators.common.s3_operator import S3CustomOperator
from airflow.models.baseoperator import BaseOperator
from operators.common.pg_operator import CustomPGOperator

import logging
import re
log = logging.getLogger(__name__)

class SapOracleOperator(BaseOperator):

    def __init__(
                self,
                task_id: str,
                task_key: str,
                oracle_cfg: dict,
                s3_cfg: dict, 
                **kwargs
                ) -> None:
        
        super().__init__(task_id=task_id,**kwargs)

        self.oracle_hook = CustomOracleHook(env=oracle_cfg['oracle_conn_id'], **kwargs)
        self.s3_operator = S3CustomOperator(s3_hook_con=s3_cfg['s3_hook_con'], bucket=s3_cfg['bucket'])
        self.s3_ingest_path = s3_cfg['s3_ingest_path']

        self.source_schema             = oracle_cfg['source_schema']
        self.user_available_list_t_tab = oracle_cfg['user_available_list_t_tab']

        self.task_key = task_key
        self.exec_method: dict = {
            'get_data_from_source': self.get_data_from_oracle
        }

    def execute(self, context: dict) -> Any:

        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res

    def get_data_from_oracle(self) -> None:

        data_of_prevs: DataFrame = self.oracle_hook.get_pandas_df(sql=f'SELECT table_name FROM {self.user_available_list_t_tab}')
        available_tables: List [str] = data_of_prevs['TABLE_NAME'].tolist()
        tables_w_data: List[str] = [table for table in available_tables if not 'FORHR' in table]
        log.info(f'The necessary tables in SAP Oracle: \n {tables_w_data}')
        
        formatted_table_names: List[str] = [s.split('/')[2] for s in tables_w_data]
        log.info(f'Table names: \n {formatted_table_names}')

        for table in tables_w_data:
            log.info(f'Table {table} is processing... ') 
            df: DataFrame = self.oracle_hook.get_pandas_df(sql=f'SELECT * FROM {self.source_schema}."{table}"')
            table_count: int = self.oracle_hook.get_first(sql=f'SELECT COUNT(*) FROM {self.source_schema}."{table}"')[0]
            log.info(f'{table_count} rows in {self.source_schema }."{table}"')
            self.s3_operator.s3_ingest_loader(
                                            ingest_path=self.s3_ingest_path,
                                            obj_to_load=df,
                                            file_name=table.split('/')[2]
                                            )


class SapOracleOdsOperator(BaseOperator):

    def __init__(
                self,
                task_id: str,
                task_key: str,
                pg_hook_con: str, 
                ods_cfg: dict, 
                **kwargs
                ) -> None:
             
        super().__init__(task_id=task_id,**kwargs)

        self.pg_operator = CustomPGOperator(pg_hook_con=pg_hook_con, stg_cfg=ods_cfg)
        self.task_key = task_key
        self.exec_method: dict = {
        'sap_oracle_ods_processor': self.pg_operator.stg_processor
        }

    def execute(self, context: dict) -> Any:

            exec_res = self.exec_method[self.task_key]()
            log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
            return exec_res