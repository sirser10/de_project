from typing import Dict, Any, List, Tuple, Union
from collections import defaultdict
from datetime import datetime, date
from pandas import DataFrame
from datetime import datetime as dt

from hooks.pg_hook import CustomPGHook
from helpers.common_helpers import DateGetterHelper
from airflow.models.baseoperator import BaseOperator

import re
import os 
import logging
import warnings

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)


class CustomPGOperator(BaseOperator):
    """
    Кастомный Postgres оператор для Airflow
    """

    def __init__(
                self,
                pg_hook_con: str,
                task_key: str=None,
                task_id: str=None,
                **kwargs
                )->None:
        
        if task_id is not None:
            super().__init__(task_id=task_id,**kwargs)
        
        self.pg_hook = CustomPGHook(env=pg_hook_con)
        self.conn = self.pg_hook.get_conn()
        self.cursor = self.conn.cursor()

        self.date_getter = DateGetterHelper()
        
        self.task_key = task_key if task_key is not None else task_id

        self.exec_method = {
            'test_m': self.test,
            'src_processor':self.src_processor,

        }

    def execute(self, context):
        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res
        

    def test(self):
        print(self.conn, self.cursor)

    def src_processor(
                    self,
                    source_obj: Any,
                    pg_src_info_cfg: dict, 
                    table_name: str, 
                    columns: dict,
                    schema: str = 'src',
                    method: str = 'truncate'
                    ) -> None:
        try:
            info_query_res: str = self.sql_information_schema(
                                                        target_schema=schema,
                                                        target_table=table_name,
                                                        **pg_src_info_cfg
                                                        )
            erase_semicolumn: str = info_query_res.split(';')[0]
            info_query_res = erase_semicolumn
            if_tab_exists_query = f"SELECT EXISTS ({info_query_res})"
            log.info(if_tab_exists_query)  
            self.cursor.execute(if_tab_exists_query)
            if_exists_table_res = self.cursor.fetchone()[0]
            log.info(f"Table exists status - {if_exists_table_res}")

            if if_exists_table_res:
                if method == 'truncate':
                    log.info(f"Table {table_name} exists. Completing TRUNCATE...")
                    self.truncate_table(schema=schema,table_name=table_name)
                    self.conn.commit()
                elif method == 'drop':
                    log.info(f"Table {table_name} exists. Completing DROP TABLE...")
                    self.drop_table(schema=schema, table_name=table_name)
                    log.info(f"Table {table_name} doesn't exist. Completing CREATE TABLE...")
                    self.create_table(schema,table_name,columns)
                    self.conn.commit()
            else:
                log.info(f"Table {table_name} doesn't exist. Completing CREATE TABLE...")
                self.create_table(schema, table_name, columns)
                self.conn.commit()
                log.info(f"Table {schema}.{table_name} has been created successfully.")
            
            current_datetime = dt.now().strftime('%Y%m%d_%H%M%S')
            filename = f'{table_name}_{current_datetime}.csv'

            if isinstance(source_obj,DataFrame):
                df: DataFrame = source_obj
                df.to_csv(filename, sep=',', index=False, encoding='utf-8')
                with open(filename, 'r', newline='', encoding='utf-8') as f:
                    self.copy_csv_to_pg_table(
                                            schema=schema, 
                                            table_name=table_name,
                                            columns=columns,
                                            file_name=f
                                        )
                os.remove(filename)

        except Exception as e:
            log.info(f"Error creating table '{schema}.{table_name}': {e}")
            self.conn.rollback()
            raise
    
    def sql_information_schema(
                                self,
                                select_column: Union[List[str],str],
                                info_table: str,
                                target_schema: Union[List[str],str],
                                target_table: Union[List[str],str],
                                ) -> str:
        
        def to_list(param: Union[List[str], str]) -> List[str]:
            return param if isinstance(param, list) else list(param.split(" "))
                
        schemas, tables, columns =  to_list(target_schema), to_list(target_table), to_list(select_column), 
        
        target_schema: str = ', '.join([f"'{s}'" for s in schemas])
        target_table: str = ', '.join([f"'{t}'" for t in tables])
        select_cols = ', '.join([f"{c}" for c in columns])

        info_schema_query = f"""
                            SELECT {select_cols}
                            FROM information_schema.{info_table} 
                            WHERE 1=1
                                AND table_schema IN ({target_schema}) 
                                AND table_name IN ({target_table})
                            ;
                        """
        return info_schema_query
    
    def truncate_table(self, schema: str, table_name: str) -> None:

        truncate_table_query = f"TRUNCATE TABLE {schema}.{table_name};" 
        log.info(truncate_table_query)
        self.cursor.execute(truncate_table_query)
        log.info(f"Table {schema}.{table_name} has been truncated.")
    
    def drop_table(self, schema: str, table_name: str) -> None:
    
        drop_table_query = f"DROP TABLE IF EXISTS {schema}.{table_name};"
        log.info(drop_table_query)
        self.cursor.execute(drop_table_query)
        log.info(f"Table {schema}.{table_name} has been dropped.")

    def drop_column(
                    self,
                    schema: str,
                    table_name: str,
                    column: Union[List[str], str],
                    ) -> None:

        alter_table_query = f"ALTER TABLE {schema}.{table_name} "
        drop_column_query = ', '.join([f"DROP COLUMN {col}" for col in column]) if isinstance(column, list) else f"DROP COLUMN {column}"
        alter_table_query += drop_column_query

        log.info(alter_table_query)
        self.cursor.execute(alter_table_query)
        log.info(f"Columns {column} has been deleted")
    
    def create_table(
                    self,
                    schema: str,
                    table_name: str, 
                    columns: Dict[str,str], 
                    ) -> None:
    
        create_table_query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ("
        for column_name, column_type in columns.items():
            create_table_query += f"{column_name} {column_type}, "

        create_table_query = create_table_query[:-2] + ")"
        log.info(create_table_query)
        self.cursor.execute(create_table_query)

    def count_table_rows(self, schema: str, table_name: str) -> int:
        try:
            count_rows_query = f"SELECT COUNT(*) FROM {schema}.{table_name};"
            self.cursor.execute(count_rows_query)
            count_rows_res: int = self.cursor.fetchone()[0]
            log.info(f"{schema}.{table_name} has {count_rows_res} rows")

            return count_rows_res
        
        except Exception as e:
            log.error(f"Error counting rows in table '{table_name}': {e}")
            raise

    def copy_csv_to_pg_table(
                            self,
                            schema: str,
                            table_name: str,
                            columns: List[str],
                            file_name: str,
                            ) -> None:
        try:
            column_list: List[str] = ", ".join(columns)
            copy_sql = f"COPY {schema}.{table_name} ({column_list}) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'"
            log.info('Launched the process of copying data from csv to postgres table...')
            self.cursor.copy_expert(sql=copy_sql, file=file_name)
            log.info(copy_sql)
            log.info(f"Data has been copied to table {schema}.{table_name} successfully.")
            self.count_table_rows(schema, table_name)
            self.conn.commit()

        except Exception as e:
            log.error(f"Error copying data to table '{schema}.{table_name}': {e}")
            self.conn.rollback()
        raise





class SQLScriptExecution(CustomPGOperator):

    def __init__(
                self,
                pg_hook_con: str,
                read_sql_cfg: dict,
                task_key=None, 
                task_id=None,
                **kwargs
                ) -> None:
        
        super().__init__(
                        pg_hook_con=pg_hook_con,
                        task_id=task_id,
                        task_key=task_key,
                        **kwargs
                        )

        self.directory     = read_sql_cfg['directory']
        self.schema_folder = read_sql_cfg['schema_folder']
        self.table_name    = read_sql_cfg['table_name']

        self.exec_method   = {'sql_script_execution' : self.sql_script_execution}


    def execute(self, context):
        return super().execute(context)

    def sql_script_execution(self) -> None:

        sql_query = self.read_sql_query(self.directory, self.schema_folder, self.table_name)
        
        try:
            self.cursor.execute(sql_query)
            self.conn.commit()
            log.info(f'Data has been successfully transfered into {self.schema_folder}-schema')

            if '.' in self.table_name:
                self.table_name = self.table_name.split('.')[1]
            self.count_table_rows(schema=self.schema_folder, table_name=self.table_name)
        except Exception as e:
            log.info("Error:", e)
            self.conn.rollback()
            raise 
        
    def read_sql_query(
                    self,
                    directory: str,
                    schema: str,
                    table_name: str
                    ) -> None:
    
        def read_file(file_path) -> None:
            with open(file_path, 'r') as file:
                return file.read()

        try:
            log.info('Start processing sql-files in directory')
            file_list: list = os.listdir(os.path.join(directory, schema))
            for file_name in file_list:

                if table_name in file_name and file_name.endswith(".sql"):
                    file_path: str = os.path.join(directory, schema, file_name)
                    log.info(f'Revelant file path is {file_path}')
                    sql_query = read_file(file_path)
                    log.info(f'SQL file for {schema}-schema was read successfully')
                    log.info(f'SQL script content:\n{sql_query}')

                    return sql_query
                
            log.info("SQL-query file executed successfully")

        except Exception as e:
            log.info(f"Error: {e}")
            raise
