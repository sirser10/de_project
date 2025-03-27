from typing import Union, Dict
from pandas import DataFrame

from operators.common.pg_operator import CustomPGOperator
from postgres_helpers.pg_datatype_detector import SQLDatatypeDetector

import logging
log = logging.getLogger(__name__)

class DWHMetaColumns:
    metacolumns = [
                'dag_id',
                'row_created_dttm', 
                'dag_started_dttm', 
                'dag_start_dttm', 
                'dag_run_id', 
                'obj_path', 
                'updated_dttm',
                'upd_dtime',
            ]

class ColumnComparisonProcessor(CustomPGOperator):

    def __init__(
                self,
                target_schema: str,
                target_table: str, 
                info_tab_cfg: str,
                schema_from: str=None, 
                df_or_table_from: Union[DataFrame, str]=None,
                is_jsonb_col_eraser: bool=False,
                **kwargs
                ) -> None:
        
        super().__init__(**kwargs)

        self.target_schema = target_schema
        self.target_table = target_table
        self.info_tab_cfg = info_tab_cfg
        self.schema_from = schema_from if schema_from else None
        self.df_or_table_from = df_or_table_from if df_or_table_from else None
        self.is_jsonb_col_eraser = is_jsonb_col_eraser if is_jsonb_col_eraser else False

        self.sql_datatype_detector = SQLDatatypeDetector(
                                                        info_tab_conf=self.info_tab_cfg,
                                                        schema=self.schema_from,
                                                        tabl=self.df_or_table_from
                                                        )

    
    def columns_comparison_processor(self) -> list:

        target_table_columns_list: list = self.extract_table_columns(
                                                                    target_schema=self.target_schema, 
                                                                    target_table=self.target_table,
                                                                    info_tab_conf=self.info_tab_cfg
                                                                    )
        log.info('Completing columns comparison processor...')

        try:
            if isinstance(self.df_or_table_from, DataFrame):

                log.info(f"DataFrame counts {len(self.df_or_table_from.columns)}")
                missing_columns = self.new_columns_check(
                                                        target_schema=self.target_schema,
                                                        target_table=self.target_table,
                                                        df_or_table_from=self.df_or_table_from,
                                                        info_tab_conf=self.info_tab_cfg
                                                        )
                if missing_columns:
                    log.info("Completing ALTER TABLE ADD COLUMN...")
                    for col in missing_columns:
                        self.add_column(
                                        schema=self.target_schema,
                                        table=self.target_table,
                                        col=col
                                    )
                        log.info(f"{col} has been added successfully to {self.target_schema}.{self.target_table}")
                else:
                    log.info(f"Number of columns in {self.target_schema}.{self.target_table} is the same as in DataFrame: {len(target_table_columns_list)} == {len(self.df_or_table_from.columns)}.")
            
            elif isinstance(self.df_or_table_from, str):
                missing_columns = self.new_columns_check(
                                                        target_schema=self.target_schema,
                                                        target_table=self.target_table,
                                                        df_or_table_from=self.df_or_table_from,
                                                        schema_from=self.schema_from,
                                                        info_tab_conf=self.info_tab_cfg
                                                        )
                if missing_columns:
                    log.info("Completing SQL datatype detection process...")

                    sql_datatype_detector_res: Dict[str, Dict[str, list]] = self.sql_datatype_detector.detection_process()
                    columns_list = [col['columns'] for tab, col in sql_datatype_detector_res.items()][0]
                    missing_colunmns_lst = [col for col in columns_list if col.split()[0] in missing_columns]

                    log.info(f'Missing column: {missing_colunmns_lst}')
                    log.info("SQL datatype detection has ended")

                    if self.is_jsonb_col_eraser:
                        log.info(f"JSONB-eraser mode is True.Erasing JSONB columns from missing columns list...")
                        missing_colunmns_lst_wo_jsonb = [col for col in columns_list if col.split()[0] in missing_columns and 'JSONB' not in col]
                        
                        for col in missing_colunmns_lst_wo_jsonb:
                            col, datatype = col.split(' ')
                            log.info("Completing ALTER TABLE ADD COLUMN without JSOB columns...")
                            self.add_column(
                                            schema=self.target_schema,
                                            table=self.target_table,
                                            col=col,
                                            type=datatype
                                            )
                            log.info(f"{col} has been added successfully to {self.target_schema}.{self.target_table}")
                    else:
                        for col in missing_colunmns_lst:
                            col, datatype = col.split(' ')
                            log.info("Completing ALTER TABLE ADD COLUMN...")
                            self.add_column(
                                            schema=self.target_schema,
                                            table=self.target_table,
                                            col=col,
                                            type=datatype
                                            )
                            log.info(f"{col} has been added successfully to {self.target_schema}.{self.target_table}")
                    return missing_colunmns_lst            
            else:
                log.info(f"Number of columns in {self.target_schema}.{self.target_table} is {len(target_table_columns_list)}")
                pass
        
        except Exception as e:
            log.info(f"Error in columns comparison  '{self.target_table}' in {self.target_schema} layer and object-from: {e}")
            self.conn.rollback()
            raise


    def new_columns_check(
                    self,
                    target_schema: str,
                    target_table: str,
                    info_tab_conf: dict,
                    schema_from: str=None,
                    df_or_table_from: Union[DataFrame, str] = None,
                    ) -> set:
    
        target_table_columns_list: list = self.extract_table_columns(
                                                                    target_schema=target_schema,
                                                                    target_table=target_table,
                                                                    info_tab_conf=info_tab_conf
                                                                    )
        missing_columns=set()

        if isinstance(df_or_table_from, DataFrame):

            df: DataFrame = df_or_table_from
            df_columns: list = df.columns
            log.info(f"{df_columns} are columns of DataFrame")

            if len(df_columns) != len(target_table_columns_list):
                missing_columns = set(df_columns) - set(target_table_columns_list)
                log.info(f"DataFrame has got missing columns regarding {target_schema}.{target_table}: \n{missing_columns}")
            else:
                log.info(f"0 new missing columns detected. Skipping...")
                pass

        if isinstance(df_or_table_from, str):
            
            metacolumns_to_remove: list = DWHMetaColumns.metacolumns
            table_columns_from: list = self.extract_table_columns(
                                                                target_schema=schema_from,
                                                                target_table=df_or_table_from,
                                                                info_tab_conf=info_tab_conf
                                                                )
            table_columns_from = [col for col in table_columns_from if col not in metacolumns_to_remove]
            if len(table_columns_from) != len(target_table_columns_list):
                missing_columns: set = set(table_columns_from) - set(target_table_columns_list) 
                log.info(f"Table has got missing columns regarding {target_schema}.{target_table}: \n{missing_columns}")
            else:
                log.info(f"0 new missing columns detected. Skipping...")
                pass
            
        return missing_columns 






