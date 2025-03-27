import logging
import warnings
from typing import Any, List, Tuple
from collections import defaultdict
from datetime import datetime, date

from operators.common.pg_operator import CustomPGOperator
from helpers.common_helpers import DateGetterHelper

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

class SQLDatatypeDetector(CustomPGOperator):

    def __init__(
                self,
                info_tab_conf: dict,
                schema: str,
                tabl: str,
                ) -> None:
        
        super.__init__(self)

        self.info_tab_conf = info_tab_conf
        self.schema = schema
        self.tabl = tabl

        self.date_getter = DateGetterHelper()


    def detection_process(self) -> dict:

        info_schema_query: str = self.sql_information_schema(
                                                    **self.info_tab_conf,
                                                    target_schema=self.schema,
                                                    target_table=self.tabl
                                                    ) 
        self.cursor.execute(info_schema_query)
        info_table_res: List[Tuple[str,]] = self.cursor.fetchall()
        log.info(f'Result of info table in sql datatype_detector: {info_table_res}')

        info_table_dict = defaultdict(list)
        for table, column in info_table_res:
            info_table_dict[table].append(column)

        try:
            log.info(f'Completing data type detection proccess...')

            table_col_dict = {}
            for table, columns in info_table_dict.items():
                metacolumns_to_remove: list  = [
                                                'dag_id',
                                                'row_created_dttm', 
                                                'dag_started_dttm', 
                                                'dag_run_id', 
                                                'obj_path', 
                                                'dag_start_dttm', 
                                                'updated_dttm',
                                                'upd_dtime',
                                                ]
                all_cols = []
                for column in columns:
                    if column in metacolumns_to_remove:
                        continue
                    
                    select_query = f"""
                                    SELECT {column}
                                    FROM {self.schema}.{table} 
                                    WHERE {column} IS NOT NULL
                                    ORDER BY LENGTH({column} :: TEXT) DESC
                                    LIMIT 1;
                                    """
                    self.cursor.execute(select_query)
                    select_distinct_res: Tuple[str,] = self.cursor.fetchone()
                    if select_distinct_res is None or select_distinct_res[0] is None:

                        all_cols.append(f"{column} TEXT")
                        log.info(f"There are some empty columns detected: {column}")
                    else:
                        select_res: Any = select_distinct_res[0]
                        all_cols.append(f"{column} {self.datatype_detector(select_res)}")
                
                table_with_cols = {f'{table}':{'columns':all_cols}}
                table_col_dict.update(table_with_cols)
                
            log.info(f'SQLDataDetector result: {table_col_dict}')
            return table_col_dict or {}

        except Exception as e:
            log.info(f"Error in datatype detection operator: {e}")
            raise

    
    def datatype_detector(self, val: Any) -> str:
        
        types_mapping = {
                        date: 'DATE',
                        int : ('INT','BIGINT',),
                        datetime:'TIMESTAMP',
                        list:'JSONB',
                        dict:'JSONB',
                        float:'DECIMAL',
        }

        if isinstance(val, str):
            if all((
                    not val.isdigit() or val[0] == '0',
                    not val.startswith(('[', '{')),         
                    not any(( self.date_getter.date_detector(val), self.date_getter.datetime_detector(val) ))  
                 )):
                return 'TEXT'
        
            elif all((
                    isinstance(val, str),
                    val.isdigit(),
                    val[0] != '0'
                  )):
                return 'INT' if len(str(val)) < 10 else 'BIGINT'
        
            elif all((
                    isinstance(val, str),
                    val.isdigit(),
                    val[0] != '0',
                    '.' or ',' in val
                )):
                return 'DECIMAL'

            elif val.startswith(('{','[')):
                return 'JSONB'
        
            elif self.date_getter.date_detector(val):
                return 'DATE'
        
            elif self.date_getter.datetime_detector(val):
                return 'TIMESTAMP'
            
        else:
            for py_t,sql_t in types_mapping.items():
                if py_t == type(val):
                    return sql_t if not isinstance(val,int) else (
                                                                sql_t[0] if len(str(val)) < 10 else sql_t[1]
                                                                )
