MANUAL_TABLE_PK = \
{
    'src.manual_dim_groups': ['group_id'],
    'src.manual_employees': ['id'],
}

"""
The script below automatically searches manual 'src' tables and its columns for stg-processing.
"""

#start
from hooks.pg_hook import pg_hook
from typing import Tuple, List
from helpers.postgres_helpers.pg_datatype_detector import SQLDatatypeDetector
import os

ENV: str = os.environ.get('ENV')
conn, cursor = pg_hook(ENV)
file_tables_info_query = f"""
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE 
                                table_schema IN ('src') 
                                AND table_name LIKE 'manual%'
                        """
cursor.execute(file_tables_info_query)
manual_tables_tpl: List[Tuple[str,]] = cursor.fetchall()
entities = [t[0] for t in manual_tables_tpl]

tab_col = {}
for table in entities:
    sql_datatype_detector = SQLDatatypeDetector(
                                                pg_hook_con=ENV,
                                                info_tab_conf={
                                                                'select_column': ['table_name', 'column_name'],
                                                                'info_table':'columns'
                                                                },
                                                schema='src',
                                                tabl=table
                            )
    detected_dict = sql_datatype_detector.detection_process()
    tab_col.update(detected_dict)

tab_col = {'src.' + k: v for k, v in tab_col.items()}
total_tab_col_dct = {
                        k: {**v, 'columns_to_deduplicate': MANUAL_TABLE_PK[k] if k in MANUAL_TABLE_PK else {}} 
                        for k, v in tab_col.items()
                }
MANUAL_TABLE_STG_CFG = {k.split('.')[1]: v for k, v in total_tab_col_dct.items()}
#end


ODS_TABLES_CFG = \
{
    'manual_dim_groups': {
        'columns':[
            'group_id INT',
            'group_name TEXT',
            'dt_start DATE',
            'dt_end DATE',
            'updated_dttm TIMESTAMP',
        ],
        'unique_constraints':['CONSTRAINT manual_dim_groups_pk PRIMARY KEY (group_id)'],
        'columns_to_upsert': [
                            'group_id',
                            'group_name',
                            'dt_start',
                            'dt_end',
                            ]
    },
    'manual_employees':{
        'columns':[
            'id INT',
            'email TEXT',
            'name TEXT',
            'surname TEXT',
            'patronymic TEXT',
            'org_unit INT',
            'position INT',
            'updated_dttm TIMESTAMP',
        ],
        'unique_constraints':['CONSTRAINT manual_employees_pk PRIMARY KEY (id)'],
        'columns_to_upsert': [
                                'id',
                                'email',
                                'name',
                                'surname',
                                'patronymic',
                                'org_unit',
                                'position',
                            ]
    }
}