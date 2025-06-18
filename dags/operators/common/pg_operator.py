from typing import Dict, Any, List, Union, Literal
from pandas import DataFrame
from datetime import datetime as dttm

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
    Кастомный Postgres оператор для Airflow, позволяющий создавать ETL внутри DWH,
    инициализировать логику автоматической загрузки данных в 3 первичные схемы (слои) Postgres:
     - src (inintal raw-слой)
     - stg (initial слой нормализованных данных)
     - ods (initial слой с дистиллированными бизнес-данными, готовыми для работы)\n
    Основываясь на этих слоях, оператор предназначен в первую очередь для вызова следующих методов:
      - src_processor
      - stg_processor
      - ods_processor
    Чтобы вызвать методы stg_processor и ods_processor, достаточно инициализровать сам класс необходимыми для методов аргументами (stg_cfg или ods_cfg)
    """
    
    def __init__(
                self,
                pg_hook_con: str,
                task_key: str=None,
                task_id: str=None,
                stg_cfg: dict=None,
                ods_cfg: dict=None,
                **kwargs
                )->None:
        
        if task_id is not None:
            super().__init__(task_id=task_id,**kwargs)
        
        self.pg_hook_con = pg_hook_con
        self.pg_hook = CustomPGHook(env=pg_hook_con)
        self.conn    = self.pg_hook.get_conn()
        self.cursor  = self.conn.cursor()

        self.stg_cfg = stg_cfg if stg_cfg else None
        self.ods_cfg = ods_cfg if ods_cfg else None
        self.kwargs  = kwargs

        self.date_getter = DateGetterHelper()

        self.task_key = task_key if task_key is not None else task_id

        self.exec_method = {
            'src_processor':self.src_processor,
            'stg_processor': self.stg_processor,
            'ods_processor': self.ods_processor
        }

    def execute(self, context):
        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res

    def src_processor(
                    self,
                    source_obj: Any,
                    pg_src_info_cfg: dict, 
                    table_name: str, 
                    columns: dict,
                    schema: str = 'src',
                    method: str = 'truncate',
                    void_replace: bool=False
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
                
                elif method == 'drop':
                    log.info(f"Table {table_name} exists. Completing DROP TABLE...")
                    self.drop_table(schema=schema, table_name=table_name)
                    log.info(f"Table {table_name} doesn't exist. Completing CREATE TABLE...")
                    self.create_table(schema,table_name,columns)
            else:
                log.info(f"Table {table_name} doesn't exist. Completing CREATE TABLE...")
                self.create_table(schema, table_name, columns)
                log.info(f"Table {schema}.{table_name} has been created successfully.")
            
            current_datetime = dttm.now().strftime('%Y%m%d_%H%M%S')
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
                if void_replace:
                    self.void_or_voidlike_col_value_removal(schema,table_name)
                self.conn.commit()
                os.remove(filename)

        except Exception as e:
            log.info(f"Error creating table '{schema}.{table_name}': {e}")
            self.conn.rollback()
            raise

    def stg_processor(self) -> None:

        schema_from: str                               = self.stg_cfg['schema_from']
        schema_to: str                                 = self.stg_cfg['schema_to']
        table_name: str                                = self.stg_cfg['table_name']
        column_list: list                              = self.stg_cfg['column_list']
        columns_to_deduplicate: list                   = self.stg_cfg['columns_to_deduplicate']
        info_tab_conf: Dict[str, Union[str,List[str]]] = self.stg_cfg['info_tab_conf']
        prefix: str                                    = self.stg_cfg.get('prefix', None)
        kwargs: dict                                   = self.kwargs if self.kwargs else None

        tab = f'{prefix}_{table_name}' if prefix else table_name
        info_schema_query = f"""
                                SELECT EXISTS (
                                            SELECT 1
                                            FROM information_schema.tables
                                            WHERE
                                                table_schema = '{schema_to}'
                                                AND table_name = '{tab}'
                                            )
                                ;
                            """
        self.cursor.execute(info_schema_query)
        log.info(info_schema_query)
        table_exists = self.cursor.fetchone()[0]
        log.info(f'Table exists status - {table_exists}')

        columns = ', '.join(column_list)
        deduplication_cols = ', '.join(columns_to_deduplicate)
        if table_exists:
            log.info(f"TABLE {schema_to}.{tab} has already EXISTS. ")
            log.info(f'Columns:\n  {columns}')
            log.info(f'Launching the process of column comparison in {schema_to}.{tab}')

            """
            CHECKING IF EXISTS NEW COLUMNS TO ADD
            """
            from helpers.postgres_helpers.pg_columns_comparison_processor import ColumnComparisonProcessor
            self.col_comparison = ColumnComparisonProcessor(
                                                            pg_hook_con=self.pg_hook_con,
                                                            target_schema=schema_to,
                                                            target_table=tab,
                                                            info_tab_cfg=info_tab_conf,
                                                            schema_from=schema_from,
                                                            df_or_table_from=tab,
                                                            )
            self.col_comparison.columns_comparison_processor()
            log.info(f'Finshed the process of column comparison in {schema_to}.{tab}')

        else:
            log.info(f"{schema_from}.{tab} doesn't exists")

            try:
                create_target_table = f"""CREATE TABLE IF NOT EXISTS {schema_to}.{tab} (row_id TEXT, {columns}, updated_dttm TIMESTAMP)"""
                log.info(create_target_table)
                self.cursor.execute(create_target_table)
                log.info(f'Table {schema_to}.{tab} has been created successfully.')
                log.info(f'Columns:\n  {columns}')

            except Exception as e:
                log.info(f"Error creating table {schema_to}.{tab}: {e}")
                self.conn.rollback()
                raise

        col_list_split: list = columns.split(' ')
        colmns_w_dtype = ', '.join(
                            [
                                f"""
                                    CASE 
                                    WHEN LENGTH({col}) =10 AND position('.' IN {col}) > 0 THEN TO_DATE({col},'DD.MM.YYYY')
                                    WHEN LENGTH({col}) =10 AND position('-' IN {col}) > 0 THEN TO_DATE({col},'YYYY-MM-DD')
                                    WHEN LENGTH({col}) =10 AND position('-' IN {col}) = 0 THEN TO_DATE({col},'YYYY-DD-MM')
                                    ELSE {col} :: DATE
                                    END :: DATE AS {col}""" if col_type.startswith('DATE')
                                else 
                                f"{col}:: VARCHAR AS {col}" if col_type.startswith('VARCHAR')
                                else 
                                f"{col}:: NUMERIC :: INT AS {col}" if col_type.startswith('INT')
                                else
                                f'{col}:: {col_type.replace(",","")} AS {col}' for col, col_type in zip(col_list_split[::2], col_list_split[1::2]) 
                            ]
                        )
        colmns_wo_dtype = ', '.join([col for col in col_list_split if not col.isupper()])
        null_availability_query = f""" SELECT EXISTS (SELECT 1 FROM {schema_from}.{tab} WHERE %s IS NULL);"""

        try:
            null_cols, notnull_cols = [], []
            for cols in columns_to_deduplicate:

                select_nulls_query = null_availability_query % cols
                log.info(select_nulls_query)

                self.cursor.execute(select_nulls_query)
                nulls_exists = self.cursor.fetchone()[0]

                if nulls_exists:
                    log.info(f'Column {cols} has NULL values')
                    null_cols.append(cols)
                else:
                    notnull_cols.append(cols)
                    
            log.info(f'Columns without NULL values: {", ".join(notnull_cols)}')
            log.info(f'Columns with NULL values: {", ".join(null_cols)}')

            if len(null_cols)>0:
                select_partition_null_cols: str = ', '.join([f"COALESCE({cols}, 'null') AS {cols}_null" for cols in null_cols])
                partition_null_cols: list = re.findall(r'\w+_null', select_partition_null_cols)
                notnull_cols.extend(partition_null_cols)

                partition_null_cols_str: str = ', '.join(partition_null_cols)
                partition_cols: str = ', '.join([cols for cols in notnull_cols])
                log.info(f'Partition columns are: {partition_cols}')
                select_all_with_type, select_all_wo_types = colmns_w_dtype + ', ' + select_partition_null_cols, colmns_wo_dtype + ', ' + partition_null_cols_str
                
            else:
                partition_cols: str = deduplication_cols
                log.info(f'Partition columns are: {partition_cols}')
                select_all_with_type, select_all_wo_types = colmns_w_dtype, colmns_wo_dtype

        except Exception as e:
            log.info(f'Error processing SELECT EXISTS execution: {e}')
            raise
        
        """
        CHECK IF JSONB COLUMNS EXISTS
        """ 
        from helpers.postgres_helpers.pg_jsonb_column_processor import JSONBColumnProcessor
        self.jsonb_processor = JSONBColumnProcessor(
                                                    info_tab_conf=info_tab_conf,
                                                    target_schema=schema_to,
                                                    target_table=tab
                                                    )
        jsonb_col_proc_res: list = self.jsonb_processor.jsonb_col_processor()
        if jsonb_col_proc_res:
            cols_from_jsonb = [c for c in [col.split(' ')[0] for col in jsonb_col_proc_res]]
            log.info(f"COLS FROM JSONB {cols_from_jsonb}")

        stg_query = f"""
                    INSERT INTO {schema_to}.{tab} (row_id, {colmns_wo_dtype}, updated_dttm)
                    WITH serialization AS(
                        SELECT 
                                {select_all_with_type}
                                , CURRENT_TIMESTAMP AS updated_dttm
                        FROM {schema_from}.{tab}
                    )
                    , deduplication AS (
                        SELECT 
                            {select_all_wo_types}
                            , updated_dttm
                            , ROW_NUMBER() OVER (PARTITION BY {partition_cols}) AS rn
                        FROM serialization 
                    )
                    SELECT 
                            MD5({'||'.join([f"{c}::TEXT" for c in notnull_cols])}) as row_id
                            , {colmns_wo_dtype}
                            , updated_dttm
                    FROM deduplication
                    WHERE rn = 1
                    ;
                """
        try:
            self.truncate_table(schema=schema_to, table_name=tab)                    
            log.info(stg_query)
            self.cursor.execute(stg_query)
            self.conn.commit()
            log.info('Generated SQL-script completed successfully.')
        except Exception as e:
            log.info(f'Error processing {schema_to}.{tab}: {e}')
            self.conn.rollback()
            raise
    

    def ods_processor(self) -> None:

        schema_from: str                               = self.ods_cfg['schema_from']
        schema_to: str                                 = self.ods_cfg['schema_to']
        table_name: str                                = self.ods_cfg['table_name']
        column_list: list                              = self.ods_cfg['column_list']
        unique_constraints: list                       = self.ods_cfg['unique_constraints']
        columns_to_upsert: list                        = self.ods_cfg['columns_to_upsert']
        info_tab_conf: Dict[str, Union[str,List[str]]] = self.ods_cfg['info_tab_conf']
        prefix: str                                    = self.ods_cfg.get('prefix',None)
        operation_type: Literal['insert', 'upsert']    = self.ods_cfg.get('operation_type', 'upsert')
        is_jsonb_col_eraser: bool                      = self.ods_cfg.get('is_jsonb_col_eraser', False)
        is_drop_column: Any                            = self.ods_cfg.get('is_drop_column', None)

        try:
            tab = f'{prefix}_{table_name}' if prefix else table_name
            if 'updated_dttm TIMESTAMP' not in column_list: 
                raise ValueError(f"Column 'updated_dttm TIMESTAMP' is missing in table '{schema_to}.{tab}' with columns: {column_list}")
            else:
                log.info(f"Table {tab} contains 'updated_dttm TIMESTAMP' in column list")

            info_schema_query = f"""
                                    SELECT EXISTS (
                                                SELECT 1
                                                FROM information_schema.tables
                                                WHERE
                                                        table_schema = '{schema_to}'
                                                        AND table_name = '{tab}'
                                                );
                                """
            self.cursor.execute(info_schema_query)
            log.info(info_schema_query)
            table_exists = self.cursor.fetchone()[0]
            log.info(f'Table exists status - {table_exists}')

            columns = ', '.join(column_list)
            unique_constraints = ', '.join(unique_constraints)

            if table_exists:
                log.info(f"TABLE {schema_to}.{tab} has already EXISTS. ")

                """
                CHECKING IF EXISTS NEW COLUMNS TO ADD
                """
                from helpers.postgres_helpers.pg_columns_comparison_processor import ColumnComparisonProcessor
                self.col_comparison = ColumnComparisonProcessor(
                                                                    pg_hook_con=self.pg_hook_con,
                                                                    target_schema=schema_to,
                                                                    target_table=tab,
                                                                    df_or_table_from=tab,
                                                                    schema_from=schema_from,
                                                                    info_tab_cfg=info_tab_conf,
                                                                    is_jsonb_cols_eraser=is_jsonb_col_eraser
                                                                    )
                added_new_columns: list = self.col_comparison.columns_comparison_processor()
            else:
                log.info(f"{schema_from}.{tab} doesn't exists")
                create_target_table = f"""CREATE TABLE IF NOT EXISTS {schema_to}.{tab} ({columns})"""
                log.info(create_target_table)
                self.cursor.execute(create_target_table)
                log.info(f'Table {schema_to}.{tab} has been created successfully.')
                log.info(f'Columns:\n  {columns}')
                log.info(f'PK:\n  {unique_constraints}')

            col_list_split: list = columns.split(' ')
            colmns_w_dtype = ', '.join(
                                [
                                    f"{col}:: DATE" if col_type == 'DATE,'
                                    else
                                    f'{col}:: {col_type.replace(",","")}' for col, col_type in zip(col_list_split[::2], col_list_split[1::2]) 
                                ]
                            )
            colmns_wo_dtype = ', '.join([col for col in col_list_split if not col.isupper()])
            constraint_pk = re.search(r'CONSTRAINT (\w+)', unique_constraints).group(1)
        
            matches = re.search(r'\((.*?)\)', unique_constraints)
            if matches:
                    columns = matches.group(1).split(', ')
                    new_constraint_str = ', '.join(columns)
            else:
                log.info('Columns have been not found')

            create_constraints_query = f"""
                                            ALTER TABLE {schema_to}.{tab}
                                            DROP CONSTRAINT IF EXISTS {constraint_pk},
                                            ADD CONSTRAINT {constraint_pk} PRIMARY KEY ({new_constraint_str});
                                        """
            try: 
                log.info(f'Creating CONSTRAINT for table {tab}...')
                log.info(create_constraints_query)
                self.cursor.execute(create_constraints_query)
                log.info(f'CONSTRAINT {constraint_pk} has been created successfully.')

            except Exception as e:
                log.info(f'Error in creating CONSTRAINT {constraint_pk}')
                raise

            if operation_type == 'upsert':

                upsert_query = f"""
                                WITH inserted_rows as (
                                    INSERT INTO {schema_to}.{tab} as target(
                                        SELECT 
                                                {colmns_wo_dtype}
                                        FROM {schema_from}.{tab}
                                        ) ON 
                                        CONFLICT ON CONSTRAINT {constraint_pk}
                                        
                                        DO UPDATE SET 
                                                    {', '.join([f'{col} = excluded.{col}' for col in columns_to_upsert ])}
                                        WHERE (
                                                CASE 
                                                    {' '.join([f'WHEN target.{col} != excluded.{col} THEN 1' for col in columns_to_upsert])}
                                                    ELSE 0
                                                END
                                            ) = 1
                                            RETURNING 1
                                        )
                                SELECT COUNT(*) FROM inserted_rows;  
                                """
                count_rows_added_query = f"SELECT COUNT(*) FROM {schema_to}.{tab};"

                try:
                    log.info(f'OPERATION TYPE = {operation_type}. Launched process of UPSERTING...')
                    log.info(upsert_query)
                    self.cursor.execute(upsert_query)
                    inserted_rows = self.cursor.fetchone()[0]
                    log.info(f"OPERATION TYPE = {operation_type}.{schema_to}.{tab} has been inserted {inserted_rows} rows")
                    self.conn.commit()
                    log.info(f'Table {schema_to}.{tab} has been upserted successfully.')

                    log.info(count_rows_added_query)
                    self.cursor.execute(count_rows_added_query)
                    count_rows_added = self.cursor.fetchone()[0]
                    log.info(f"{schema_to}.{tab} has {count_rows_added} row in total.")
                except Exception as e:
                    log.info(f'Error processing upsert table {schema_to}.{tab}: {e}')
                    self.conn.rollback()
                    raise
            else:
                count_rows_added_query = f"SELECT COUNT(*) FROM {schema_to}.{tab};"
                insert_query = f"""
                                DROP TABLE IF EXISTS {schema_to}.{tab};
                                {create_target_table}
                                {create_constraints_query}
                                INSERT INTO {schema_to}.{tab} ({colmns_wo_dtype})
                                SELECT {colmns_wo_dtype} FROM {schema_from}.{tab}
                                ;
                                """
                try:
                    log.info(f'OPERATION TYPE = {operation_type}. Launched process of INSERTING...')
                    log.info(insert_query)
                    self.cursor.execute(insert_query)
                    inserted_rows = self.cursor.fetchone()[0]
                    log.info(f"{schema_to}.{tab} has been inserted {inserted_rows} rows")
                    
                    self.conn.commit()
                    log.info(f'Table {schema_to}.{tab} has been upserted successfully.')

                    log.info(count_rows_added_query)
                    self.cursor.execute(count_rows_added_query)
                    count_rows_added = self.cursor.fetchone()[0]
                    log.info(f"{schema_to}.{tab} has {count_rows_added} row in total.")
                except Exception as e:
                    log.info(f'Error processing upsert table {schema_to}.{tab}: {e}')
                    self.conn.rollback()
                    raise
            #update added columns if they appeared
                if added_new_columns:
                    self.update_added_columns(
                                        schema_from=schema_from,
                                        table_from=tab,
                                        target_schema=schema_to,
                                        target_table=tab,
                                        columns_to_update=added_new_columns,
                                        keys_col_list=unique_constraints
                    )
            #DROP EXTRA COLUMN(S) FEATURE
            if is_drop_column:
                log.info('is_drop_column mode activated. Need to delete particalur columns!')
                tab_col_dct = {tab:col for tab, col in is_drop_column.items()}
                for t, drop_c in tab_col_dct.items():
                        if tab == prefix+t:
                            self.cursor.execute(
                                            self.sql_information_schema(
                                                                   select_column='column_name',
                                                                   info_table='columns',
                                                                   target_schema=schema_to,
                                                                   target_table=prefix+t
                                                                    )
                                        )
                            fetch_res: tuple = self.cursor.fetchall()
                            if isinstance(drop_c, list):
                                exists = any(any(col in tpl for tpl in fetch_res) for col in drop_c)
                            else:
                                exists = any(drop_c in tpl for tpl in fetch_res)
                            
                            if exists:
                                table_name = tab
                                log.info(f'Deleting column {drop_c}...')
                                self.drop_column(
                                            schema=schema_to,
                                            table_name=table_name,
                                            column=drop_c,
                                            )
                                self.conn.commit()
                        else:
                            log.info(f'Table {tab} has not got columns to delete. Skip...')
                            
        except Exception as e:
            log.info(f"Error processing ods tables:{e}")
            raise
        finally:
            self.cursor.close()
            self.conn.close()

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

    def extract_table_columns(
                        self,
                        target_schema: str,
                        target_table: str,
                        info_tab_conf: dict,
                        ) -> List[str]:
        try:
            columns_select_query = self.sql_information_schema(
                                                        **info_tab_conf,
                                                        target_schema=target_schema,
                                                        target_table=target_table
                                                        )
            
            self.cursor.execute(columns_select_query)
            table_columns: List[str] = [col for tab, col in self.cursor.fetchall()]
            return table_columns
        
        except Exception as e:
            log.error(f"Error in SELECT '{target_schema}.{target_table}': {e}")
            raise
    
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
    
    def add_column(
                self,
                schema: str,
                table: str, 
                col: str, 
                type: str='TEXT'
                ) -> None:

        add_column_query = f"ALTER TABLE {schema}.{table} ADD COLUMN {col} {type};"
        log.info(add_column_query)
        self.cursor.execute(add_column_query)

    def create_table(
                    self,
                    schema: str,
                    table_name: str, 
                    columns: Dict[str,str], 
                    ) -> None:
    
        create_table_query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ("
        for column_name, column_type in columns.items():
            create_table_query += f"{column_name} {column_type}, "

        create_table_query = create_table_query[:-2] + ");"
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

        except Exception as e:
            log.error(f"Error copying data to table '{schema}.{table_name}': {e}")
            raise

    def update_added_columns(
                            self,
                            schema_from: str,
                            table_from:str,
                            target_schema: str,
                            target_table: str,
                            columns_to_update: list,
                            keys_col_list: list,
                            ) -> None:

        set_columns_list = [c.split(' ')[0] for c in columns_to_update]
        unique_constraints = ', '.join(keys_col_list)

        matches = re.search(r'\((.*?)\)', unique_constraints)
        keys_list = matches.group(1).split(', ') if matches is not None else log.info('Columns have been not found')
        update_by_keys_query = f""" 
                                WITH updated_rows AS (
                                    UPDATE {target_schema}.{target_table} AS {target_schema} 
                                    SET
                                        {', '.join([f'{set_col}={schema_from}.{set_col}' for set_col in set_columns_list])}
                                    FROM {schema_from}.{table_from} AS {schema_from}
                                    WHERE 
                                            {' AND '.join([f'{target_schema}.{col} = {schema_from}.{col}' for col in keys_list])}
                                )
                                SELECT COUNT(*) FROM updated_rows
                                ; 
                                """
        log.info(update_by_keys_query)
        try:
            count_rows_res_before = self.count_table_rows(schema_from,table_from)
            self.cursor.execute(update_by_keys_query)
            updated_rows_count = self.cursor.fetchone()[0]

            log.info(f"Count of UPDATED rows = {updated_rows_count}. Columns {set_columns_list} has been UPDATED")

            count_rows_res_updated = self.count_table_rows(target_schema,target_table)
            if count_rows_res_before == count_rows_res_updated:
                log.info(f"Count rows in {target_schema}.{target_table} BEFORE UPDADE is the same as AFTER UPDATE = {count_rows_res_before}")
                log.info("UPDATE process has completed successfully.")
            else:
                log.info(f"Count before UPDATE: {count_rows_res_before}\nCount after UPDATE: {count_rows_res_updated}")
                raise ValueError("Row count mismatch after UPDATE.")
        except Exception as e:
            log.error(f"An error occurred: {e}")
            self.conn.rollback()
            raise

    def void_or_voidlike_col_value_removal(
                                        self,
                                        schema: str,
                                        table: str,
                                        ) -> None:
        try:
            inforamtion_columns_query: List[str] = self.sql_information_schema(
                                                                            select_column='column_name',
                                                                            info_table='columns',
                                                                            target_schema=schema,
                                                                            target_table=table,
                                                                            )
            self.cursor.execute(inforamtion_columns_query)
            inforamtion_columns_list: List[str] = [col[0] for col in self.cursor.fetchall()]

            empty_symbols = ['', ' ', 'N/A', 'None']
            empty_symbols_str = ", ".join(f"'{symbol}'" for symbol in empty_symbols)

            for col in inforamtion_columns_list:
                query_search_columns = f"SELECT EXISTS (SELECT 1 FROM {schema}.{table} WHERE {col} IN ({empty_symbols_str}) LIMIT 1);"
                log.info(f"Executing SELECT EXISTS: {query_search_columns}")
                self.cursor.execute(query_search_columns)
                exists = self.cursor.fetchone()[0]
                update_query = f"""
                                    UPDATE {schema}.{table}
                                    SET {col} = NULL 
                                    WHERE {col} IN ({empty_symbols_str})
                                    ;
                                """
                if exists:
                    log.info(f"Void/Voidlike symbols has been detected. Executing {update_query}")
                    self.cursor.execute(update_query)
                else:
                    log.info(f"The result of Void/Voidlike symbols existing: '{exists}'. No Void/Voidlike symbols have not been detected.")
        except Exception as e:
            log.error(f"Error while processing column in table {schema}.{table}: {e}")
            self.conn.rollback()
            raise