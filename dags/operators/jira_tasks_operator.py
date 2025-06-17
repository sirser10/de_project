from typing import Dict, Any, List, Tuple
from requests import Response
from pandas import DataFrame

from hooks.api_hook import CustomAPIHook
from operators.common.pg_operator import CustomPGOperator
from helpers.common_helpers import CommonHelperOperator
from airflow.models.baseoperator import BaseOperator

import logging
import warnings
import pandas as pd

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

class JiraTasksOperator(BaseOperator):

      def __init__(
                  self,
                  task_id: str,
                  task_key: str,
                  conn_id: str,
                  api_extraction_params: dict,
                  pg_cfg: dict, 
                  **kwargs
                  ) -> None:
          
          super().__init__(task_id=task_id,**kwargs)

          self.api_hook              = CustomAPIHook(conn_id=conn_id)
          self.common_helper         = CommonHelperOperator()
          self.pg_operator           = CustomPGOperator(pg_hook_con=pg_cfg['pg_hook_con'])
          self.pg_cfg                = pg_cfg
          self.api_extraction_params = api_extraction_params

          self.task_key = task_key if task_key is not None else task_id
          self.exec_method: dict = {'extract_jira_to_db': self.extract_jira_to_db}
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
      

      def extract_jira_to_db(self) -> None:
            
            api_cfg_w_params      = self.api_extraction_params.copy()
            api_cfg               = api_cfg_w_params['api_cfg']
            response_issue_fields = api_cfg_w_params['response_issue_fields']
            creds                 = api_cfg_w_params['creds']
            
            start_at              = int(api_cfg['params'].get('startAt_', 'startAt=1').split('=')[1])
            page: int             = start_at
            config_fields_set     = set(response_issue_fields)

            data = []
            while True:
                  api_cfg['params']['startAt_'] = 'startAt=%d' % page
                  request_cfg = {
                                    'endpoint': api_cfg['endpoint'],
                                    'params': api_cfg['params'],
                                    'headers':{
                                                'Authorization': creds['token'],
                                                'Content-Type': creds['content_type']
                                    },
                                    'method': 'GET',
                                    'status_error_handler': True,
                  }
                  response: Response = self.api_hook.get_restapi_response(**request_cfg)
                  if response.status_code == 200:
                        data_from_response: List[Dict[str,Any]] = response.json().get('issues',{})
                        for issue in data_from_response:
                              fields = issue.get('fields', {})
                              filtered_fields  = {key: fields[key] for key in fields if key in config_fields_set}
                              issue['fields'] = filtered_fields
                              data.append(issue)
                        page += 50
                  elif response.status_code == 429: #awaited error, so we quit the loop and move next
                        break
                  else:
                        log.error(f"Unexpected status code {response.status_code}. Stopping the request loop.")
                        raise Exception(f"Request failed with status code: {response.status_code}")

            
            df: DataFrame = pd.DataFrame(data)
            df['fields'] = df['fields'].apply(lambda x: pd.Series(x).to_json())
            log.info(f"DataFrame of response is \n{df}")
            
            df: DataFrame = df.assign(**self.common_helper.get_metadata_columns(context=self.dag_context))
            columns: dict = {col: 'TEXT' for col in df.columns}
            prefix: str = self.pg_cfg['prefix']

            self.pg_operator.src_processor(
                                          source_obj=df,
                                          table_name=prefix + '_' + self.pg_cfg['table_name'],
                                          columns=columns
            )

class JiraOdsProcessor(BaseOperator):

      def __init__(
                self,
                task_id: str,
                task_key: str,
                pg_hook_con: str, 
                ods_cfg: dict, 
                **kwargs
                ) -> None:
            
            super().__init__(task_id=task_id,**kwargs)

            self.pg_operator = CustomPGOperator(pg_hook_con=pg_hook_con)
            self.task_key = task_key
            self.ods_cfg = ods_cfg
            self.exec_method: dict = {'jira_ods_processor': self.jira_ods_processor}
      

      def execute(self, context: dict) -> Any:

            exec_res = self.exec_method[self.task_key]()
            log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
            return exec_res
      

      def jira_ods_processor(self) -> None:

            conn, cursor = self.pg_operator.conn, self.pg_operator.cursor
            schema_from, target_schema, table_name = self.ods_cfg['schema_from'], self.ods_cfg['schema_to'], self.ods_cfg['table_name']
            jsonb_field: str = self.ods_cfg['jsonb_field']
            rename_config: dict = self.ods_cfg['rename_config']

            columns_search_query = self.pg_operator.sql_information_schema(
                                                                        select_column=['column_name', 'data_type'],
                                                                        info_table='columns',
                                                                        target_schema=schema_from,
                                                                        target_table=table_name
                                                                        )
            log.info(columns_search_query)
            cursor.execute(columns_search_query)
            main_stg_cols_list = list(map(lambda x: f"{x[0]} {x[1].upper()}", cursor.fetchall()))
            main_stg_cols = [
                        r for r in [i.split(' ')[0] for i in main_stg_cols_list] 
                        if r not in ['updated_dttm', jsonb_field]
                        ]
            log.info(f"Columns in {table_name}:\n{main_stg_cols}")

            main_cols_str = ','.join([f"{col} as {col}" for col in main_stg_cols])
            select_table_query = f""" 
                                    SELECT json_keys
                                    FROM 
                                    {schema_from}.{table_name}
                                    , jsonb_object_keys({jsonb_field}) as json_keys                  
                              """
            log.info(select_table_query)
            cursor.execute(select_table_query)
            json_keys: List[Tuple[str,]] = cursor.fetchall()
            keys: list = list(set(item for i in json_keys for item in i))

            log.info(f"The number of keys is {len(keys)}. JSON keys are \n{keys}")
            
            jsonb_columns, text_colums =[],[]
            for k in keys:
                  not_null_value_query=f"""
                                    SELECT 
                                          DISTINCT ({jsonb_field} ->> '{k}') AS {k}
                                    FROM {schema_from}.{table_name} 
                                    WHERE ({jsonb_field} ->> '{k}') IS NOT NULL
                                    LIMIT 1
                              """
                  cursor.execute(not_null_value_query)
                  non_null_values = cursor.fetchone()

                  for i, value in enumerate(non_null_values):
                        try:
                              if isinstance(value,dict) or isinstance(value,str) and value.startswith('{'):
                                    jsonb_columns.append(cursor.description[i].name)
                              else:
                                    text_colums.append(cursor.description[i].name)
                        except (ValueError, TypeError):
                              pass
            log.info(f"""
                        \n Number of JSONB columns = {len(jsonb_columns)}\
                        \n Columns with JSONB array: {jsonb_columns}\
                        \n\n Number of TEXT columns = {len(text_colums)}\
                        \n Columns with TEXT datatype {text_colums}
                        """
                        )
            col_json_renamed = [rename_config[col] if col in rename_config else col for col in jsonb_columns]
            col_text_renamed = [rename_config[col] if col in rename_config else col for col in text_colums]
            
            main_filtered_col_list = [col for col in main_stg_cols_list if col not in ['updated_dttm TIMESTAMP WITHOUT TIME ZONE', 'fields JSONB']]
            main_cols_table: str =  ','.join(main_filtered_col_list)     
            jsonb_cols_table: str = ','.join([f"{col} JSONB " for col in col_json_renamed])
            text_cols_table: str = ','.join([f"{col} TEXT " for col in col_text_renamed])

            table_to = target_schema + '.' + table_name
            create_ods_table_query = f"""CREATE TABLE IF NOT EXISTS {table_to} (
                                                                              {main_cols_table}
                                                                              , {text_cols_table}
                                                                              , {jsonb_cols_table}
                                                                              , updated_dttm timestamp
                                                                        )
                                                                        ;
                                    """
            constraint_pk='jira_tasks_pk'
            create_constraints_query = f"""
                              ALTER TABLE {table_to}
                              DROP CONSTRAINT IF EXISTS {constraint_pk},
                              ADD CONSTRAINT {constraint_pk} PRIMARY KEY (row_id);
                             """
            try:
                  log.info(create_ods_table_query)
                  log.info(create_constraints_query)
                  cursor.execute(create_ods_table_query)
                  cursor.execute(create_constraints_query)
                  log.info(f'Table {table_to} has been created successfully.')
                  log.info(f'CONSTRAINT {constraint_pk} has been created successfully.')


                  conn.commit()
            except Exception as e:
                  log.info(f'Error in creating table  {table_to}:{e}')
                  conn.rollback()
                  raise
            
            keys_to_cols: str = ','.join([f"""({jsonb_field} ->> '{key}') AS {rename_config.get(key,key)}""" for key in keys])
            jsonb_cols: str = ','.join([f"{col} :: JSONB " for col in col_json_renamed])
            text_cols: str = ','.join([f"{col} :: TEXT" for col in col_text_renamed])

            cols_to_upsert: list  = col_json_renamed + col_text_renamed + main_stg_cols

            upsert_query = f"""
                              INSERT INTO {table_to} as target(
                                    WITH cte AS (
                                          SELECT 
                                                  {main_cols_str}
                                                , {keys_to_cols}
                                                , current_timestamp as updated_dttm
                                          FROM {schema_from}.{table_name}
                                    )
                                    SELECT 
                                            {main_cols_str}
                                          , {text_cols}
                                          , {jsonb_cols}
                                          , updated_dttm
                                    FROM cte
                        ) ON
                        CONFLICT ON CONSTRAINT {constraint_pk}

                        DO UPDATE SET 
                                    {', '.join([f'{col} = excluded.{col}' for col in cols_to_upsert])}
                        WHERE (
                                CASE 
                                    {' '.join([f'WHEN target.{col} != excluded.{col} THEN 1' for col in cols_to_upsert])}
                                    ELSE 0
                                END
                              ) = 1
                        ;
                        """
            try:
                  log.info(f'Launched process of UPSERTING...')
                  log.info(upsert_query)
                  
                  cursor.execute(upsert_query)
                  
                  log.info(f'Table {table_to} has been upserted successfully.')
                  self.pg_operator.count_table_rows(schema=target_schema, table_name=table_name)
                  
                  conn.commit()
            except Exception as e:
                  log.info(f'Error processing upsert table {table_to}: {e}')
                  conn.rollback()
                  raise


      






      