from typing import (
                    Dict,
                    List,
                    Union,
                    Any,
                    Literal
                )
from pandas import DataFrame
from datetime import datetime as dttm
from io import StringIO, BytesIO

from airflow.models.baseoperator import BaseOperator
from hooks.s3_hook import CustomS3Hook
from operators.common.pg_operator_c import CustomPGOperator
from customs.helpers.common_helpers import CommonHelperOperator

import pandas as pd
import logging
import os
import chardet
import io 
import fastparquet as fp

log = logging.getLogger(__name__)

class PandasColumnTypeMapping:
    pd_int_type_mapper = {
        'object':'Int64',
        'int64':'Int64',
    }

class S3CustomOperator(BaseOperator):

    def __init__(
                self,
                s3_hook_con: str, 
                bucket: str,
                task_key: str=None,
                task_id: str=None, 
                ingest_cfg: dict=None,
                stage_cfg: dict=None,
                distilled_cfg: dict=None,
                pg_cfg: dict = None,
                **kwargs
                ) -> None:
        
        if task_id is not None:
            super().__init__(task_id=task_id,**kwargs)
        
        self.s3_hook = CustomS3Hook(env=s3_hook_con)
        self.bucket  = bucket

        self.ingest_cfg = ingest_cfg if ingest_cfg else None
        self.stage_cfg = stage_cfg if stage_cfg else None 
        self.distilled_cfg = distilled_cfg if distilled_cfg else None   

        self.kwargs = kwargs

        self.common_helper     = CommonHelperOperator()
        self.pg_cfg            = pg_cfg

        self.task_key = task_key if task_key is not None else task_id
        self.exec_method: dict = {
                                    's3_stage_processor': self.s3_stage_processor,
                                    's3_distilled_processor': self.s3_distilled_processor,
                                    's3_loading_to_db_processor': self.s3_loading_to_db_processor
        }
        self.dag_context = {key: None for key in ['dag_id', 'dag_run_id', 'dag_start_dttm']}
        
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

    def read_s3bucket_obj(self,bucket: str, source_path: str) -> List[str]:
        return self.s3_hook.list_keys(bucket, prefix=source_path)

    def rename_columns( 
                        self,
                        file_name: str,
                        df: DataFrame,
                        rename_dict: Dict[str, Dict[str,str]]
                        ) -> DataFrame:

        file_name_wo_ext: str = self.common_helper.get_file_name_wo_ext(file_name)
        df: DataFrame = df.rename(columns=rename_dict[file_name_wo_ext]) if file_name_wo_ext in rename_dict else df
        return df

    def deduplicate_dataframe(
                            self,
                            df: DataFrame,
                            config: Dict[str, Union[str, List[str]]],
                            file_name: str
                            ) -> DataFrame:
        """
        Выполняет дедупликацию DataFrame на основе конфига.

        Параметры на вход:
        df (pandas.DataFrame): DataFrame.
        config (dict): Конфиг-файл, содержащий необходимые ключи:
            - 'key_columns': Список столбцов, по которым будет производиться дедубликация.
            - 'sort_columns': Столбец, по которому будет производиться сортировка.
        
        Возвращает дедуплицированный DataFrame.
        """
        try:
            if file_name in config:

                key_columns: Union[str, List[str]] = config[file_name]['key_columns']
                sort_columns: Union[str, List[str]] = config[file_name]['sort_columns']
                log.info(f'Key columns for deduplication are: {key_columns}. Sorting by {sort_columns}')

                if isinstance(key_columns, str):
                    key_columns = [key_columns]
                    
                if isinstance(sort_columns, str):
                    sort_columns = [sort_columns]
                
                df['row_num'] = (
                                    df
                                    .sort_values(sort_columns, ascending=False)
                                    .groupby(key_columns)
                                    .cumcount() + 1
                                )
                
                df_deduplicated = df[df.row_num == 1]
                df_distilled = df_deduplicated.drop('row_num', axis=1)
                log.info('Deduplication has been complited succesfully')
                log.info(f'The rows number in DataFrame before distillation - {len(df)}')
                log.info(f'The rows number in DataFrame after distillation - {len(df_distilled)}')
                if len(df) != len(df_distilled):
                    log.info(f'The rows number (before deduplication = {len(df)}) and (after deduplication = {len(df_distilled)}) does not match')
                    log.info(f"{len(df) - len(df_distilled)} rows are duplicates")
                return df_distilled
            else:
                return df
        except Exception as e:
            log.info(f"Error: {e}")
            raise e

    def get_df_column_dtypes(self, df: DataFrame) -> List[dict]:

        df_column_dtypes = ', '.join([f'{col} : {dtype}' for col, dtype in df.dtypes.items()])
        log.info('Columns of Dataframe:\n%s', list({df_column_dtypes}))
        
        return list({df_column_dtypes})

    def df_read_csv(self, string_object: StringIO, **kwargs) -> DataFrame:
        try:
            if isinstance(string_object, StringIO):
                file_content = string_object.read().encode()
                result = chardet.detect(file_content)
                encoding_res = result['encoding']

                possible_seps = [",", ";", "\t", "|", " ", '"', "'", "#", "/", ".", ",", "<", ">", "?", "~", "@", ":", ",", "}", "{", "]", "[", "+", "=", "-", "_"]
                string_object.seek(0)

                sample = file_content[:1000]
                sep_res = None
                for sep_res in possible_seps:
                    try:
                        df: DataFrame =pd.read_csv(
                                                    string_object,
                                                    nrows=1,
                                                    sep=sep_res,
                                                    engine='python'
                                                    )
                        if len(df.columns) > 1:
                            log.info(f"Detected separator: '{sep_res}'")
                            break
                    except ValueError:
                        continue
                    else:
                        string_object.seek(0)
                        df: DataFrame = pd.read_csv(io.BytesIO(sample), nrows=1, engine='python')
                        if len(df.columns) == 1:
                            sep_res = df.columns[0]
                            log.info(f"Detected separator: '{sep_res}'")

            string_object.seek(0)
            df_final: DataFrame = pd.read_csv(
                                            string_object,
                                            encoding=encoding_res,
                                            sep=sep_res,
                                            **kwargs
                                        )
            log.info(f'Initial cols number: {len(df_final.columns)}')
            unnamed_cols = [col for col in df_final.columns if 'Unnamed' in col]
            if unnamed_cols:
                df_final = df_final.drop(unnamed_cols, axis=1)
                log.info(f"Dropped columns: {unnamed_cols}")
            log.info(f'Cols number after column were dropped: {len(df_final.columns)}')

            return df_final
        except Exception as e:  
            log.error(f"Error reading CSV file: {e}")
            raise

    def df_read_parquet(self, paquet_buffer: BytesIO) -> DataFrame:
        try: 
            if isinstance(paquet_buffer, BytesIO):
                df = pd.read_parquet(path=paquet_buffer)
                return df
        except Exception as e:
            log.info('Error while reading parquet file:', e)
            raise

    def write_to_parquet(
                        self,
                        df: DataFrame,
                        file_loc: str,
                        compression='snappy'
                        ) -> None:

        try:
            if isinstance(df, DataFrame) and file_loc.split('.')[-1] == 'parquet':
                fp.write(
                        filename=file_loc,
                        data=df,
                        compression=compression,
                        append=False
                    )
        except Exception as e:
            log.info(f'Error while writing DataFrame to parquet: {e}')
            raise


    def read_s3_file(
                    self,
                    s3_obj: Any,
                    bucket: str, 
                    encoding_detector: bool=True,
                    ) -> Any:
        try:
            s3_get_file = self.s3_hook.get_key(s3_obj, bucket)
            response = s3_get_file.get()
            file_content = response['Body'].read()

            if encoding_detector:
                result = chardet.detect(file_content)
                encoding = result['encoding']
                log.info(f"Detected encoding of {s3_obj}: {encoding}")
                file_content = file_content.decode(encoding)

            return file_content
        except Exception as e:
            log.error(f"Error reading file {s3_obj}: {e}")
            raise

    def write_df_to_parquet(
                            self,
                            df_to_write: DataFrame,
                            file_name: str,
                            s3_key: str,
                            bucket_name: str,
                            write_mode: Literal['overwrite', 'append']='overwrite'
                            ) -> None:
        try:
            s3_key_parquet: str = s3_key + '.parquet'
            file_name_parquet: str = file_name + '.parquet'

            log.info(f'Writing DataFrame to {s3_key_parquet}...')

            if write_mode =='overwrite':

                log.info(f'Writing mode: {write_mode}') 
                self.write_to_parquet(df=df_to_write, file_loc=file_name_parquet)
                self.s3_hook.load_file(
                                        filename=file_name_parquet,
                                        key=s3_key_parquet,
                                        bucket_name=bucket_name,
                                        replace=True
                                    )
                log.info(f'File has been successfully overwrited to S3 bucket {s3_key_parquet}') 
                os.remove(file_name_parquet)
            else:

                log.info(f'Writing mode: {write_mode}')

                existing_s3_key_parquet: str = s3_key_parquet
                read_existing_parquet = self.read_s3_file(
                                                        s3_obj=existing_s3_key_parquet,
                                                        bucket=bucket_name,
                                                        encoding_detector=False
                                                        )
                parquet_buffer: BytesIO = io.BytesIO(read_existing_parquet)
                df_existing: DataFrame = self.df_read_parquet(parquet_buffer)
                df_appended: DataFrame = pd.concat(
                                                    [df_existing, df_to_write],
                                                    ignore_index=True
                                                    )
                log.info(', '.join([
                                    f'Rows number currently: {df_existing.shape[0]}',
                                    f'Rows number till append: {df_to_write.shape[0]}',
                                    f'Rows number after append: {df_appended.shape[0]}'
                                    ]))

                self.write_to_parquet(df=df_appended,file_loc=file_name_parquet)
                self.s3_hook.load_file(
                                        filename=file_name_parquet,
                                        key=s3_key_parquet,
                                        bucket_name=bucket_name,
                                        replace=True
                                    )
                log.info(f'File has been successfully appended to S3 bucket {s3_key_parquet}')
                os.remove(file_name_parquet)
        except Exception as e:
            log.info(f'Error occured while writing parquet: {e}')
            raise

    def void_check(self, sorted_arr: list) -> Union[bool, str]:
        if isinstance(sorted_arr, list):
            empty_symbols = ['', ' ', 'N/A']
            find_null_value: bool|str = sorted_arr[0] if sorted_arr[0] in empty_symbols else False
            if find_null_value:
                void_symbol: str = find_null_value
                return void_symbol
            return False
        else:
            ValueError('Input arg is not list. Put the list instead.')   

    def void_or_nulls_column_replace(self, df: DataFrame) -> None:
        
        col_lst = df.columns.to_list()
        for col in col_lst:
            uniq_col_vals = [str(v) for v in df[col].unique()]
            uniq_col_vals.sort()
            void_symbol = self.void_check(uniq_col_vals)
            if void_symbol:
                log.info(f"Column {col} has void values: '{void_symbol}'")
                df[col] = df[col].replace(regex=f"^{void_symbol}$", value=None)

    def s3_ingest_loader(
                        self,
                        ingest_path: str,
                        obj_to_load: Any,
                        file_name: str,
                        ) -> None:
        
        try:
            log.info('s3 ingest loader has been launched...')
            if isinstance(obj_to_load, DataFrame):
                obj_to_load.to_csv(
                                    file_name,
                                    sep=',',
                                    index=False, 
                                    encoding='utf-8'
                                    ) 
                self.s3_hook.load_file(
                                        filename=file_name,
                                        key=ingest_path + file_name + '.csv',
                                        bucket_name=self.bucket,
                                        replace=True
                                    )
                log.info(f"File has been written to {ingest_path + file_name + '.csv'} successfully.")
        except Exception as e:
            log.info(f'Error in processing ingest object: {e}')
            raise
    
    def get_col_types_as_dct(self, df: DataFrame) -> dict:
        return df.dtypes.astype(str).to_dict()
    
    def int_col_mapping_process(self, df: DataFrame) -> None:
      
        pd_mapping = PandasColumnTypeMapping()
        int_map = dict(pd_mapping.pd_int_type_mapper)
        
        col_types_dct = self.get_col_types_as_dct(df)
        for col_name, type_ in col_types_dct.items():
            
            uniq_values_lst = [str(v) for v in df[col_name].unique()]
            uniq_values_lst.sort()
            if type_ in int_map and all([v.isnumeric() for v in uniq_values_lst if v !='None']):
                    df[col_name] = df[col_name].apply(lambda x: str(x) if pd.notnull(x) else x).astype(int_map[type_])

    def s3_stage_processor(self) -> None:
        
        source_path: str                           = self.stage_cfg['source_path']
        dest_path: str                             = self.stage_cfg['dest_path']
        rename_col_mapping: dict                   = self.stage_cfg.get('rename_col_mapping',{})
        file_name_mapping: dict                    = self.stage_cfg.get('file_name_mapping',{})
        write_format: Literal['csv', 'parquet']    = self.stage_cfg['write_format']
        write_mode: Literal['append', 'overwrite'] = self.stage_cfg.get('write_mode', None)
        void_replace_mode: bool                    = self.stage_cfg.get('void_replace_mode',False)
        pd_col_type_mapping: bool                  = self.stage_cfg.get('pd_col_type_mapping', False)
        kwargs                                     = self.kwargs if self.kwargs else None

        read_ingest_files: list = self.read_s3bucket_obj(self.bucket, source_path)
        log.info(f'Ingest-layer objects: \n{read_ingest_files}')

        if len(read_ingest_files) > 0:
            for file in read_ingest_files:
                    
                log.info(f'The processig of {file} has been started')

                s3_content: Any = self.read_s3_file(s3_obj=file, bucket=self.bucket)
                string_object: StringIO = StringIO(s3_content)
                metadata_columns = self.common_helper.get_metadata_columns(s3_obj_path=file, context=self.dag_context)
                df: DataFrame = self.df_read_csv(string_object)
                self.void_or_nulls_column_replace(df) if void_replace_mode else df
                self.int_col_mapping_process(df) if pd_col_type_mapping else df
                df_formatted: DataFrame = (
                                self.rename_columns(
                                                    file_name=file,
                                                    df=df,
                                                    rename_dict=rename_col_mapping if rename_col_mapping else {}
                                                    )
                                                    .assign(**metadata_columns)
                )
                                                                
                df_column_dtypes: str = ', '.join([f'{col} : {dtype}' for col, dtype in df_formatted.dtypes.items()])
                log.info('Columns of Dataframe:\n%s', list({df_column_dtypes}))

                proper_file_name = self.common_helper.get_file_name_from_config(file, file_name_mapping) if file_name_mapping else self.common_helper.get_file_name_wo_ext(file)
                log.info(f'File name - {proper_file_name}')
                if write_format == 'parquet':
                    self.write_df_to_parquet(
                                            df_to_write=df_formatted,
                                            file_name=proper_file_name,
                                            s3_key=dest_path+proper_file_name,
                                            bucket_name=self.bucket,
                                            write_mode=write_mode
                                        )
                elif write_format == 'csv':
                    csv_buffer = StringIO()
                    df_formatted.to_csv(csv_buffer, sep=',', index=False, encoding='utf-8')
                    self.s3_hook.load_string(
                                        string_data=csv_buffer.getvalue(),
                                        key=dest_path + proper_file_name + '.csv',
                                        bucket_name=self.bucket,
                                        replace=True
                                        )
                    log.info(f'File has been successfully loaded to S3 bucket {dest_path}{proper_file_name}.csv')

    def s3_distilled_processor(self) -> None:
        
        source_path: str                        = self.distilled_cfg['source_path']
        dest_path: str                          = self.distilled_cfg['dest_path']
        distillation_proc_cfg: dict             = self.distilled_cfg['distillation_proc_cfg']
        write_format: Literal['csv', 'parquet'] = self.distilled_cfg.get('write_format', 'parquet')
        write_mode: Literal['overwrite']        = self.distilled_cfg.get('write_mode', 'overwrite')
        
        try:
            read_files: list = self.read_s3bucket_obj(self.bucket, source_path)
            if len(read_files) > 0:

                if write_format == 'csv':
                    read_csv_files = [csv for csv in read_files if '.csv' in csv]
                    for file in read_csv_files:
                        log.info(f'The distillation processig of {file} has been started')

                        s3_content = self.s3_hook.read_key(file, self.bucket)
                        string_object: StringIO =StringIO(s3_content)
                        df: DataFrame = pd.read_csv(string_object)

                        df_column_dtypes: str = ', '.join([f'{col} : {dtype}' for col, dtype in df.dtypes.items()])
                        log.info(f'Columns of Dataframe: {list({df_column_dtypes})}')

                        distilled_file_name = self.common_helper.get_file_name_wo_ext(file)
                        log.info(f"Primary keys for DataFrame: {distillation_proc_cfg[distilled_file_name]['key_columns']}")

                        df_distilled: DataFrame = self.deduplicate_dataframe(df=df, config=distillation_proc_cfg, file_name=distilled_file_name)

                        filename = f"data_{dttm.now().strftime('%Y%m%d_%H%M%S')}.csv"
                        df_distilled.to_csv(filename, sep=',', index=False, encoding='utf-8')
                        self.s3_hook.load_file(
                                                filename=filename,
                                                key=dest_path + distilled_file_name + '.csv',
                                                bucket_name=self.bucket,
                                                replace=True
                                            )
                        log.info(f'File has been successfully loaded to S3 bucket {dest_path}{distilled_file_name}.csv')
                        os.remove(filename)
                else:
                    read_parquet_files = [parquet for parquet in read_files if '.parquet' in parquet]
                    for file in read_parquet_files:
                        log.info(f'The distillation processig of {file} has been started')
                        s3_content: Any = self.read_s3_file(
                                                            s3_obj=file,
                                                            bucket=self.bucket,
                                                            encoding_detector=False
                                                            )
                        parquet_buffer: BytesIO = io.BytesIO(s3_content)
                        df: DataFrame = self.df_read_parquet(parquet_buffer)

                        distilled_file_name: str = self.common_helper.get_file_name_wo_ext(file)
                        log.info(f"Primary keys for DataFrame: {distillation_proc_cfg[distilled_file_name]['key_columns']}")
                        df_distilled: DataFrame = self.deduplicate_dataframe(df, distillation_proc_cfg, distilled_file_name)
                        self.write_df_to_parquet(
                                                df_to_write=df_distilled,
                                                file_name=distilled_file_name,
                                                s3_key=dest_path+distilled_file_name,
                                                bucket_name=self.bucket,
                                                write_mode=write_mode
                                            )
        except Exception as e:
            log.error(f'Error has occured while processing distillation: {e}')
            raise


    def create_primary_pgtable_name(
                                    self,
                                    file_name: str,
                                    prefix: str,
                                    table_type: str= None
                                    ) -> str:
        prefix_pattern = f'{prefix}_{table_type}_' if table_type else f'{prefix}_'
        return prefix_pattern + self.common_helper.get_file_name_wo_ext(file_name)           

    def s3_loading_to_db_processor(self) -> None:

        pg_hook_con: str                       = self.pg_cfg['pg_hook_con']
        source_path: str                       = self.pg_cfg['source_path']
        prefix: str                            = self.pg_cfg['prefix']
        read_format: Literal['csv', 'parquet'] = self.pg_cfg.get('read_format', 'parquet')
        erase_method: str                      = self.pg_cfg.get('erase_method', 'truncate')
        pg_src_info_cfg: dict                  = self.pg_cfg['pg_src_info_cfg']
        schema: str                            = self.pg_cfg.get('schema', 'src')
        void_replace: bool                     = self.pg_cfg.get('void_replace', False)                          
        kwargs                                 = self.kwargs if self.kwargs else None

        read_distilled_obj: List[str] = self.read_s3bucket_obj(self.bucket, source_path)
        if len(read_distilled_obj) > 0:

            for obj in read_distilled_obj:
                log.info(f'The processig of {obj} has been started')
                if read_format == 'csv':
                
                    s3_content = self.s3_hook.read_key(obj, self.bucket)
                    string_object: StringIO =StringIO(s3_content)
                    df: DataFrame = pd.read_csv(string_object)
                else:
                    s3_content = self.read_s3_file(
                                                s3_obj=obj,
                                                bucket=self.bucket,
                                                encoding_detector=False
                                                )
                    parquet_buffer: BytesIO = io.BytesIO(s3_content)
                    df: DataFrame = self.df_read_parquet(parquet_buffer)

                table_name = self.create_primary_pgtable_name(file_name=obj,prefix=prefix)
                columns: dict = {col: 'TEXT' for col in df.columns}

                self.pg_operator = CustomPGOperator(pg_hook_con=pg_hook_con)
                self.pg_operator.src_processor(
                                                source_obj=df,
                                                pg_src_info_cfg=pg_src_info_cfg,
                                                table_name=table_name,
                                                columns=columns,
                                                schema=schema,
                                                method=erase_method,
                                                void_replace=void_replace,
                                                )