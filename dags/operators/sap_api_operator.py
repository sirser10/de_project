import logging
import pandas as pd
import datetime

from datetime import datetime,timedelta
from typing import List, Dict, Any
from pandas import DataFrame
from requests import Response
from concurrent.futures import ThreadPoolExecutor, as_completed
from hooks.api_hook import CustomAPIHook
from operators.common.pg_operator import CustomPGOperator
from helpers.common_helpers import CommonHelperOperator
from airflow.models.baseoperator import BaseOperator


log = logging.getLogger(__name__)

class SapAPIOperator(BaseOperator):

    def __init__(
                self,
                task_id: str,
                task_key: str,
                conn_id: str, 
                sap_config: dict,
                pg_cfg: dict,
                legal_sap_cfg: dict,
                **kwargs
                ) -> None:
        
        super().__init__(task_id=task_id,**kwargs)

        self.api_hook         = CustomAPIHook(conn_id=conn_id)
        self.common_helper    = CommonHelperOperator()
        self.pg_operator      = CustomPGOperator(pg_hook_con=pg_cfg['pg_hook_con'])
        self.sap_config: dict = sap_config
        self.pg_cfg: dict     = pg_cfg
        self.legal_sap_cfg: dict = legal_sap_cfg

        self.task_key: str = task_key
        self.exec_method: dict = {'fetch_data_from_source': self.fetch_data_from_source}
        self.dag_context: dict = {key: None for key in ['dag_id', 'dag_run_id', 'dag_start_dttm']}

    def execute(self, context: dict) -> Any:

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
         

    def fetch_data_from_source(self) -> None:

        prefix: str                            = self.pg_cfg['prefix']
        pg_src_info_cfg: dict                  = self.pg_cfg['pg_src_info_cfg']

        response_from_source: Response = self.api_hook.get_restapi_response(**self.sap_config)
        data_from_response: Dict[str, Any] = response_from_source.json().get('output', {})

        output_dfs: Dict[str, DataFrame] = {}
        if self.task_id in ('org_structure', 'jira_org_structure'):
             
            for key in data_from_response.keys():
                df: DataFrame = DataFrame(data_from_response.get(key, data_from_response))
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].str.encode('latin-1').str.decode('utf-8')
                if self.task_id == 'jira_org_structure': output_dfs[f"jira_{key}"] = df
                else: output_dfs[key] = df

            if self.task_id == 'org_structure':
                """
                Ввиду специфики ручки, юридическую структуру можно забрать из другой ручки, подставляя каждый табельник в ручку через цикл.
                Поэтому мы отдельно выделяем эту сущность в рамках org_structure, процессим и добавляем в общий словарь к остальным датафреймам
                """
                legal_struct_key: str = 'legal_struct'
                df_legal_struct: DataFrame = pd.DataFrame(self.fetch_legal_data_from_source(output_dfs['employees']))
                df_legal_struct: DataFrame = df_legal_struct.rename(columns=self.legal_sap_cfg['columns_to_rename'])
                column_prefixes = {
                            'position': 'position_',
                            'org_unit': 'org_unit_',
                            'employee_group': 'employee_group_',
                            'employee_category': 'employee_category_',
                            'cost_center': 'cost_center_'
                }
                legal_struct_normalized_dfs = []
                for column, prefix in column_prefixes.items():
                    normalized_df: DataFrame = pd.json_normalize(df_legal_struct[column]).add_prefix(prefix)
                    legal_struct_normalized_dfs += [normalized_df]

                sap_legal_employees_struct = pd.concat([df_legal_struct] + legal_struct_normalized_dfs, axis=1)
                output_dfs[legal_struct_key] = sap_legal_employees_struct

        else:
            df: DataFrame = pd.DataFrame(data_from_response)
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].str.encode('latin-1').str.decode('utf-8')
            default_key: str = 'bu' if self.task_id == 'bu_structure' else self.task_id
            output_dfs[default_key] = df
    
        
        for entity, df in output_dfs.items():
            df: DataFrame = df.assign(
                                        **self.common_helper.get_metadata_columns(context=self.dag_context)
                                    )
            columns: dict = {col: 'TEXT' for col in df.columns}
            self.pg_operator.src_processor(
                                            source_obj=df,
                                            pg_src_info_cfg=pg_src_info_cfg,
                                            table_name=prefix + "_dim_" + entity,
                                            columns=columns
            )

    def fetch_legal_data_from_source(self, employees_df: DataFrame) -> List[Any]:

        tabel_ids: list = employees_df['id']
        list_of_tabel_id = [i for i in tabel_ids]

        num_threads: int = 4
        chunk_size: int  = 1000
        yesterday_date: str  = (datetime.now().date()- timedelta(days=1)).strftime("%Y-%m-%d")
        response_data_list = []

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i in range(0,len(list_of_tabel_id),chunk_size):
                chunk = list_of_tabel_id[i:i+chunk_size]
                futures.append(
                            executor.submit(
                                                self.process_chunk_for_legal_structure,
                                                chunk=chunk,
                                                date=yesterday_date
                            )
                )
            
            for future in as_completed(futures):
                try:
                    response_data_list.extend(future.result())
                except Exception as e:
                    log.info(f"Error processing chunk: {e}")
        return response_data_list

    
    def process_chunk_for_legal_structure(
                                        self,
                                        date: str, 
                                        chunk: list,
                                        ) -> List[dict]:
        
        log.info(f"Processing chunk started")
        date_template: str   = self.legal_sap_cfg['params']['date_param']
        entity_template: str = self.legal_sap_cfg['params']['entity_param']

        url_with_tabel_list: List[str] = [
            self.api_hook.construct_url(
                endpoint=self.legal_sap_cfg['endpoint'],
                params_config={
                    'entity_param': entity_template.format(pernr),
                    'date_param': date_template.format(date)
                }
            )
            for pernr in chunk
        ]
        
        responses: List[Response] = [
            self.api_hook.get_restapi_response(
                                                url=url,
                                                method=self.legal_sap_cfg['method'],
                                                auth=self.legal_sap_cfg['auth'],
            ) for url in url_with_tabel_list
        ]
        data_list: List[dict] = [response.json() for response in responses if response.status_code ==200]

        return data_list
