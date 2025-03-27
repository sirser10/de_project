from typing import List, Tuple

from operators.common.pg_operator import CustomPGOperator
from postgres_helpers.pg_datatype_detector import SQLDatatypeDetector

import logging
log = logging.getLogger(__name__)

class JSONBColumnProcessor(CustomPGOperator):

    def __init__(
                self,
                info_tab_conf: dict,
                target_schema: str,
                target_table: str,
                rename_jsnb_cols={},
                **kwargs
                ) -> None:
        
        super().__init__(**kwargs)

        self.info_tab_conf    = info_tab_conf
        self.target_schema    = target_schema
        self.target_table     = target_table
        self.rename_jsnb_cols = rename_jsnb_cols if rename_jsnb_cols else {}

        self.sql_datatype_detector = SQLDatatypeDetector(   
                                                    info_tab_conf=self.info_tab_conf,
                                                    schema=self.target_schema,
                                                    tabl=self.target_table
                                                    )
    def jsonb_col_processor(self) -> list:

        table_col_dict: dict = self.sql_datatype_detector.detection_process()
        main_columns_dtp_list: list = table_col_dict[self.target_table]['columns']
        jsonb_cols_check =[col for col in main_columns_dtp_list if 'JSONB' in col]

        if not jsonb_cols_check:
            return []
        else:
            jsonb_cols: dict =\
                {
                    key: {'columns': [col for col in value['columns'] if 'JSONB' in col]} for key, value in table_col_dict.items()
                }
            list_of_jsonb_typed_columns = [v for tab, columns in jsonb_cols.items() for v in columns['columns']]
            list_of_jsonb_columns =  [c for c in [col.split(' ')[0] for col in list_of_jsonb_typed_columns]]
            log.info(f"Columns in {self.target_schema}.{self.target_table}:\n{list_of_jsonb_columns}")
            
            select_json_keys_query =f"""
                                    SELECT DISTINCT json_keys
                                    FROM {self.target_schema}.{self.target_table}
                                    , jsonb_object_keys(%s :: JSONB) AS json_keys
                                    """
            list_of_all_columns = []
            for jsonb_c in list_of_jsonb_columns:
                _select_json_keys_query= select_json_keys_query % jsonb_c
                log.info(_select_json_keys_query)

                self.cursor.execute(_select_json_keys_query)
                json_keys: List[Tuple[str,]] = self.cursor.fetchall()
                keys: list = list(set(item for i in json_keys for item in i))
                log.info(f"The number of keys is {len(keys)}. JSON keys are {keys}")
                
                jsonb_columns, text_columns =[],[]
                for k in keys:
                    jsnb_alias = jsonb_c + '_'+ k
                    not_null_value_query=f"""
                                        SELECT 
                                            ({jsonb_c} :: JSONB ->> '{k}') AS {jsnb_alias}
                                        FROM {self.target_schema}.{self.target_table} 
                                        WHERE ({jsonb_c} :: JSONB ->> '{k}') IS NOT NULL
                                        LIMIT 1
                                    """
                    log.info(not_null_value_query)
                    self.cursor.execute(not_null_value_query)
                    non_null_values = self.cursor.fetchone()

                    for i, value in enumerate(non_null_values):
                        try:
                            if isinstance(value, dict) or isinstance(value, str) and value.startswith('{'):
                                jsonb_columns.append(self.cursor.description[i].name)
                            else:
                                text_columns.append(self.cursor.description[i].name)
                        except (ValueError, TypeError):
                            raise

                    log.info(f"""
                                \n Number of JSONB columns = {len(jsonb_columns)}\
                                \n Columns with JSONB array: {jsonb_columns}\
                                \n\n Number of TEXT columns = {len(text_columns)}\
                                \n Columns with TEXT datatype {text_columns}
                            """)
            if self.rename_jsnb_cols:
                col_json_renamed = [self.rename_jsnb_cols[col] if col in self.rename_jsnb_cols else col for col in jsonb_columns]
                col_text_renamed = [self.rename_jsnb_cols[col] if col in self.rename_jsnb_cols else col for col in text_columns]
            else:
                col_json_renamed, col_text_renamed=jsonb_columns, text_columns
            
            jsonb_cols_table: list = [f"{col} JSONB " for col in col_json_renamed]
            text_cols_table: list = [f"{col} TEXT " for col in col_text_renamed]
            list_of_all_columns.extend(text_cols_table + jsonb_cols_table)
    
        res = list_of_all_columns + list_of_jsonb_typed_columns

        return res or []
                    







        