from manatal_project.helpers.common_helpers import CommonHelperOperator
from manatal_project.operators.pg_operator import CustomPGOperator
from airflow.models.baseoperator import BaseOperator

from typing import Dict, Any, Tuple 
from io import StringIO 
from pandas import DataFrame
from numpy import ndarray


import logging
import os
import hashlib
import csv
import logging
import chardet
import pandas as pd
import re 
import numpy as np

log = logging.getLogger(__name__)

class CustomFilesOperator(BaseOperator):

    def __init__(
                self,
                task_id: str,
                task_key: str, 
                dir_path: str,
                pg_src_cfg: dict,
                rename_col_mapping: dict=None,
                **kwargs
                ) -> None:
        
        super().__init__(task_id=task_id,**kwargs)

        self.dir_path           = dir_path
        self.pg_src_cfg         = pg_src_cfg
        self.rename_col_mapping = rename_col_mapping

        self.common_helper      = CommonHelperOperator()
        self.pg_operator        = CustomPGOperator(pg_hook_con=self.pg_src_cfg['pg_hook_con'])

        self.task_key = task_key
        self.exec_method = {
            'find_and_load_file_to_db': self.find_and_load_file_to_db,
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
    
    def find_and_load_file_to_db(self) -> None:
        
        pg_src_info_cfg: dict    = self.pg_src_cfg['pg_src_info_cfg']
        dir_path: str            = self.dir_path
        rename_col_mapping: dict = self.pg_src_cfg.get('rename_col_mapping', {})
        file_name_mapping: dict  = self.pg_src_cfg.get('file_name_mapping', {})
        prefix: str              = self.pg_src_cfg.get('prefix', {})
        erase_method: str        = self.pg_src_cfg.get('erase_method', 'truncate')

        all_files_lst: list = os.listdir(dir_path)
        csv_files_lst, xlsx_files_lst = [dir_path + csv for csv in all_files_lst if '.csv' in csv], [dir_path + xlsx for xlsx in all_files_lst if '.xlsx' in xlsx]

        if len(csv_files_lst) > 0:
            for csv_ in csv_files_lst:
                log.info(f"Processing of {csv_.split('/')[-1]}...")
                df: DataFrame = self.normilized_csv_to_df(csv_)
                df_csv, pg_columns = self.df_base_operator(
                                                        df=df,
                                                        metadata_columns=self.common_helper.get_metadata_columns(s3_obj_path=csv_, context=self.dag_context),
                                                        file_name=csv_,
                                                        rename_col_mapping=rename_col_mapping
                                                        )         
                
                proper_csv_file_name: str = self.common_helper.get_file_name_from_config(csv_, file_name_mapping) if file_name_mapping and csv_.split('/')[-1].split('.')[0] in file_name_mapping else self.common_helper.get_file_name_wo_ext(csv_)
                log.info(f'New propper file name - {proper_csv_file_name}')

                self.pg_operator.src_processor(
                                            source_obj=df_csv,
                                            pg_src_info_cfg=pg_src_info_cfg,
                                            table_name=prefix + '_' + proper_csv_file_name if prefix else proper_csv_file_name,
                                            columns=pg_columns,
                                            method=erase_method
                                        )
                log.info(f"{csv_} was successfully loaded to DWH")

        if len(xlsx_files_lst) > 0:
            for xlsx_ in xlsx_files_lst:
                df: DataFrame = pd.read_excel(io=xlsx_)
                df_xlsx, pg_columns = self.df_base_operator(
                                                            df=df,
                                                            metadata_columns=self.common_helper.get_metadata_columns(s3_obj_path=xlsx_, context=self.dag_context),
                                                            file_name=xlsx_,
                                                            rename_col_mapping=rename_col_mapping
                                                            )
                proper_xlsx_file_name: str = self.common_helper.get_file_name_from_config(xlsx_, file_name_mapping) if file_name_mapping else self.common_helper.get_file_name_wo_ext(xlsx_)
                log.info(f'New propper file name - {proper_xlsx_file_name}')

                self.pg_operator.src_processor(
                                            source_obj=df_xlsx,
                                            pg_src_info_cfg=pg_src_info_cfg,
                                            table_name=prefix + '_' + proper_xlsx_file_name if prefix else proper_xlsx_file_name,
                                            columns=pg_columns,
                                            method=erase_method,
                                            )
                log.info(f"{xlsx_} was successfully loaded to DWH")
                

    def encoding_detector(self, string_object: str, **kwargs) -> str:

        if isinstance(string_object, StringIO):
            
            file_content: bytes = string_object.read().encode()
            detected_result: dict = chardet.detect(file_content)
            encoding_res: str = detected_result['encoding']
            log.info(f'Encoding: {encoding_res}')

            return encoding_res

    def separator_detector(self, string_object: StringIO, **kwargs) -> str:

        possible_seps = [",", ";", "\t", "|", " ", '"', "'", "#", "/", ".", ",", "<", ">", "?", "~", "@", ":", ",", "}", "{", "]", "[", "+", "=", "-", "_"]
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
                    separator=sep_res
                    break
            except ValueError as e:
                log.info(f'Error occured while detecting separators: {e}')
                raise
        return separator

    def normilized_csv_to_df(self, csv_path: str) -> None:

        def get_col_types_as_dct(df: DataFrame) -> dict:
            return df.dtypes.astype(str).to_dict()
        
        def remove_errors(df: DataFrame, column: str) -> None:
            if any(df[column].astype(str).str.contains(r'#ERROR!', na=False)):
                    df[column] = df[column].replace('#ERROR!', np.nan)
        
        def float_to_int_col_values_converter(df: DataFrame, col_name: str) -> None:

            uniq_notnull_vals: ndarray = df[col_name].loc[~df[col_name].isnull()].unique()
            uniq_notnull_vals_lst = [str(val) for val in uniq_notnull_vals]
            
            if all(re.search('^\d+\.0$', val) for val in uniq_notnull_vals_lst):
                df[col_name] = df[col_name].apply(lambda x: str(x).split('.')[0] if pd.notnull(x) else x).astype('Int64')                

        try:
            log.info('Layunched csv normalizer processor...')
            if isinstance(csv_path, str):
                log.info('CSV file has been found')

                with open(csv_path, mode='r') as f:
                    csv_file = csv.reader(f)
                    row_count = sum(1 for _ in csv_file)
                    f.seek(0)
                    first_five_rows = [next(csv_file) for _ in range(min(row_count,101))]
                    csv_string: str = "\n".join([",".join(row) for row in first_five_rows])

                    string_object: StringIO = StringIO(csv_string)
                    encoding_res: str = self.encoding_detector(string_object)
                
                    string_object.seek(0)
                    sep_res: str = self.separator_detector(string_object)

                df_final: DataFrame = pd.read_csv(
                                                    filepath_or_buffer=csv_path,
                                                    encoding=encoding_res,
                                                    sep=sep_res,
                                                )
                log.info(f'Initial cols number: {len(df_final.columns)}')

                unnamed_cols = [col for col in df_final.columns if 'Unnamed' in col]
                df_final.drop(unnamed_cols, axis=1) if unnamed_cols else df_final
                log.info(f'Dropped columns: {unnamed_cols}')
                log.info(f'Cols number after column were dropped: {len(df_final.columns)}')

                col_types = get_col_types_as_dct(df_final)
                for col, _type in col_types.items():
                    if _type == 'float64':
                        float_to_int_col_values_converter(df_final,col)

                    remove_errors(df=df_final,column=col)
                log.info(f'Updated column types {df_final.dtypes.astype(str).to_dict()}')

                return df_final
            
            log.info('Csv normalizer processor has ended succesfully...')

        except Exception as e:
            log.info(f'Error has occured while processing csv files: {e}')
            raise


    def get_file_name_wo_ext(self, file_name: str) -> str:

        file_name_full: str   = file_name.split('/')[-1]
        file_name_wo_ext: str = '.'.join(file_name_full.split('.')[:-1])
        return file_name_wo_ext

    def rename_columns( 
                        self,
                        df: DataFrame,
                        file_name: str = None,
                        rename_dict: Dict[str, Dict[str,str]] = None
                        ) -> DataFrame:
        if file_name:
            file_name_wo_ext = self.get_file_name_wo_ext(file_name)
            if file_name_wo_ext in rename_dict:
                df = df.rename(columns=rename_dict[file_name_wo_ext])
        else:
            df
        return df

    def df_base_operator(
                        self,
                        df: DataFrame,
                        metadata_columns: dict,
                        file_name: str= None,
                        rename_col_mapping=None,
                    ) -> Tuple[DataFrame, dict]:
        
        df: DataFrame = self.rename_columns(df=df, file_name=file_name, rename_dict=rename_col_mapping) if rename_col_mapping else df
        df: DataFrame = df.assign(**metadata_columns)
        pg_columns: dict = {col: 'TEXT' for col in df.columns}

        return df, pg_columns
    

class FileHashDetector(BaseOperator):

    def __init__(
                self,
                task_id: str,
                task_key: str, 
                dir_path: str,
                hash_file_path: str,
                **kwargs
                )->None:
        
        super().__init__(task_id=task_id,**kwargs)
        
        self.dir_path          = dir_path
        self.hash_file_path    = hash_file_path
        self.task_key          = task_key
        self.exec_method: dict = {
            'execute_filehash_change': self.file_change_hash_check,
        }

        self.dag_context = {}

    def execute(self, context: dict) -> Any:

        self.dag_context['ti'] = context['ti']

        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res
    
    def file_change_hash_check(self) -> None:
        
        current_hashes = {}
        for filename in os.listdir(self.dir_path): 
            filepath = os.path.join(self.dir_path, filename)
            current_hashes[filepath] = self.get_file_hash(filepath)

        saved_hashes = self.load_hashes(self.hash_file_path)
        changes_detected = False

        for file_path, current_hash in current_hashes.items():
            saved_hash = saved_hashes.get(file_path)
            if saved_hash is None or current_hash != saved_hash:
                changes_detected = True
                log.info(f'Changes in files {file_path} have been detected.')
                break  # changes detected = true, quit cycle
            else:
                log.info(f'No changes in {file_path}.')

        self.save_hashes(self.hash_file_path, current_hashes) 
        self.dag_context['ti'].xcom_push(key='changes_detected', value=changes_detected)

    def get_file_hash(self, file_path: str) -> Any:
        """Getting file hash"""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()

    def load_hashes(self, hash_file_path: str) -> dict:
        """Loading saved file hashes"""
        if os.path.exists(hash_file_path):
            with open(hash_file_path, 'r') as f:
                return dict(line.strip().split() for line in f)
        return {}

    def save_hashes(self, hash_file_path: str, hashes: Any) -> None:

        with open(hash_file_path, 'w') as file:
            for path, h in hashes.items():
                file.write(f"{path} {h}\n")