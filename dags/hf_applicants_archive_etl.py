import json
import os
from datetime import datetime, timedelta
from copy import deepcopy
import logging
import csv
import pandas as pd
import gc

log = logging.getLogger(__name__)
IS_ENV_DEV = False

if 'IS_ENV_DEV' in os.environ:
    IS_ENV_DEV = bool(os.environ.get('IS_ENV_DEV'))
    if IS_ENV_DEV:
        log.info('Starting with dev environment')

from datetime import datetime

from airflow import DAG
from airflow import settings as airflow_settings
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
from globals import globals
from globals.utils import VariableGetter

dct_applicants           = 'src.hfarchive_dct_applicants'
fct_applicant_vacancies  = 'src.hfarchive_fct_applicant_vacancies'
fct_applicant_statuses   = 'src.hfarchive_fct_applicant_statuses'
fct_applicant_logs       = 'src.hfarchive_fct_applicant_logs'
fct_applicant_tags       = 'src.hfarchive_fct_applicant_tags'
fct_applicant_resumes    = 'src.hfarchive_fct_applicant_resumes'
fct_applicant_duplicates = 'src.hfarchive_fct_applicant_duplicates'

def unzip(zip_path: str, zip_to_path: str, pwd: any):
    # from stream_unzip import stream_unzip

    if isinstance(pwd, VariableGetter):
        pwd = pwd.get()

    if not isinstance(pwd, str):
        raise ValueError('Not correct format of password')

    return BashOperator(
        task_id       = 'unzip_file_bash',
        bash_command  = f'7z e -y {zip_path} -o{zip_to_path} -p{pwd}'
    ).execute({})

def find_value(data, key):

    if isinstance(data, dict):  
        if key in data: 
            return data[key]
        for value in data.values():
            result = find_value(value, key)
            if result is not None: 
                return result
    elif isinstance(data, list):  
        for item in data: 
            result = find_value(item, key)
            if result is not None: 
                return result
    return None 


REPLACEABLE_CHARS_MAPPING = {
    "'": '"',
    '\n': ' ',
    '\r': ' ',
    '\x00': ''
}
def replace_txt(txt: any, replace_exceptions: list = None):

    _txt = deepcopy(txt)

    if isinstance(txt, (list, dict)):
        _txt = json.dumps(_txt, ensure_ascii=False)

    if not isinstance(_txt, str):
        _txt = str(_txt)

    _replaceable = deepcopy(REPLACEABLE_CHARS_MAPPING)

    if replace_exceptions is not None:
        [_replaceable.pop(key) for key in replace_exceptions]

    for k, v in _replaceable.items():
        _txt = _txt.replace(k, v)

    return _txt


def get_attr_else_err(obj: any, attr: any, return_if_error: any = None):

    _obj = deepcopy(obj)
    if isinstance(attr, list):
        _attr = deepcopy(attr)

    elif isinstance(attr, str):
        _attr = [attr]
    else:
        raise TypeError('Attribute of not appropriate type')

    try:
        for _a in _attr:
            #if list, then each _a is a path of possible values
            #so algo is to iterate until finding first not return_if_error
            if isinstance(_a, list):
                _res = get_attr_else_err(obj, _a, return_if_error)
                if _res != return_if_error:
                    return _res
                continue

            if isinstance(_obj, dict):
                _obj = _obj[_a]
            else:
                _obj = getattr(_obj, _a)

    except:
        return return_if_error

    return _obj

def yield_json_object(path_to_folder: str, file_name: str, encoding: str = 'utf-8', chunk_size: int = 1000):
    json_path = os.path.join(path_to_folder, file_name + '.json')
    with open(json_path, 'r', encoding=encoding) as file:
        chunk = []
        for line in file:
            chunk.append(line.strip())
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def pg_creating_table(table_name: str, conn, cursor):
    
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (\
                                                        row_id SERIAL PRIMARY KEY,\
                                                        row_created_ddtm TIMESTAMP,\
                                                        content_hash VARCHAR(32) GENERATED ALWAYS AS (md5(row_id::TEXT)) STORED,\
                                                        content jsonb,\
                                                        dag_id VARCHAR(32),\
                                                        dag_run_id TEXT,\
                                                        dag_started_dttm TIMESTAMP\
                                                        )")
    conn.commit()
    log.info(f'Table {table_name} has been created successfully')

def counting_rows_added(table_name : str, cursor):

    try:

        count_rows_added_query = f"SELECT COUNT(*) FROM {table_name};"
        cursor.execute(count_rows_added_query)
        count_rows_added = cursor.fetchone()[0]
        log.info(f"{table_name} has been inserted {count_rows_added} rows")
        return count_rows_added

    except Exception as e:
        logging.error(f"Error counting rows in table '{table_name}': {e}")
        return None


def import_json_to_postgres(data, table_names):

    hook = PostgresHook(postgres_conn_id="pg-conn")
    log.info('Initializing hook')
    conn = hook.get_conn()
    log.info('Hook initialized')
    cursor = conn.cursor()


    current_time = datetime.now()
    dag_id = 'hfarchive_applicants_etl'
    log.info('Applicant JSON-file start processing')
    data_gen = yield_json_object(
                                path_to_folder=data['path_to_folder'], 
                                file_name=data['file_name'], 
                                chunk_size=data['chunk_size']
                                )
    
    def execute_truncate(cursor, table_names):
            truncate_query = f"TRUNCATE {', '.join(table_names)};"
            cursor.execute(truncate_query)
            log.info(f"Truncated the following tables: {', '.join(table_names)}")

    tables_to_truncate = [
                              dct_applicants 
                            , fct_applicant_vacancies
                            , fct_applicant_statuses
                            , fct_applicant_duplicates
                            , fct_applicant_logs
                            , fct_applicant_tags
                            , fct_applicant_resumes
                            ]
    execute_truncate(cursor, tables_to_truncate)
    conn.commit()

    for chunk in data_gen:
        log.info(f'Data is processing and importing to postgres...')
        data = [json.loads(x) for x in chunk]


        #importing table with applicants dict 

        pg_creating_table(dct_applicants, conn, cursor)
        insert_query_dct_applicants = f"INSERT INTO {dct_applicants} (row_created_ddtm, dag_started_dttm, dag_id, content) VALUES (%s, %s, %s, %s)"

        insert_dct_applicants = []
        for x in data:
            applicant_id                = int(x['id'])
            applicant_questionary_jsonb = json.dumps(get_attr_else_err(x, ['questionary_values'], []),ensure_ascii=False)
            # cv = x['external']
            
            content = {
                'applicant_id': applicant_id,
                'applicant_questionary_jsonb': applicant_questionary_jsonb,
                # 'cv': cv
            }
            
            insert_dct_applicants.append((current_time, current_time, dag_id, json.dumps(content)))
            
        cursor.executemany(insert_query_dct_applicants, insert_dct_applicants)
        conn.commit()
        log.info('Data has been imported to PostgreSQL successfully.')
        counting_rows_added(dct_applicants, cursor)


        #importing table with applicant vacancies

        pg_creating_table(fct_applicant_vacancies, conn, cursor)
        insert_query_fct_applicant_vacancies = f"INSERT INTO {fct_applicant_vacancies} (row_created_ddtm, dag_started_dttm, dag_id, content) VALUES (%s, %s, %s, %s)"
  
        insert_fct_applicant_vacancies = []
        for x in data:

            vacancy = None
            applicant_vacancy_status_id = None
            applicant_vacancy_rejection_reason_id = None
            applicant_vacancy_changed_dtime = None
            applicant_vacancy_updated_dtime = None
            applicant_source_code = None

            applicant_id = int(x['id'])
            vacancy_id = x['vacancies']
            
            for l in vacancy_id:
                vacancy = l['vacancy']
                applicant_vacancy_status_id = l['status']
                applicant_vacancy_rejection_reason_id = find_value(l, 'rejection_reason')
                applicant_vacancy_changed_dtime = find_value(l, 'changed')
                applicant_vacancy_updated_dtime = find_value(l, 'updated')
                applicant_source_code = find_value(l, 'source')

            content = {
                'applicant_id': applicant_id,
                'vacancy_id': vacancy_id,
                'vacancy' : vacancy,
                'applicant_vacancy_status_id': applicant_vacancy_status_id,
                'applicant_vacancy_rejection_reason_id': applicant_vacancy_rejection_reason_id,
                'applicant_vacancy_changed_dtime': applicant_vacancy_changed_dtime,
                'applicant_vacancy_updated_dtime': applicant_vacancy_updated_dtime,
                'applicant_source_code': applicant_source_code
            }

            
            insert_fct_applicant_vacancies.append((current_time, current_time, dag_id, json.dumps(content)))
        
        cursor.executemany(insert_query_fct_applicant_vacancies, insert_fct_applicant_vacancies)
        conn.commit()
        log.info('Data has been imported to PostgreSQL successfully.')
        counting_rows_added(fct_applicant_vacancies, cursor) 


        #importing table with applicant statuses

        pg_creating_table(fct_applicant_statuses, conn, cursor)
        insert_query_fct_applicant_statuses = f"INSERT INTO {fct_applicant_statuses} (row_created_ddtm, dag_started_dttm, dag_id, content) VALUES (%s, %s, %s, %s)"

        logs_statuses = None
        insert_fct_applicant_statuses = []

        for l in data:
            applicant_id = int(l['id'])
            logs_statuses = l['logs']

            logggs = []
            
            for logg in logs_statuses:
                logs_content = {}


                vacancy_id = None 
                applicant_vacancy_new_status_id = None
                applicant_vacancy_new_rejection_reason_id = None
                applicant_vacancy_new_status_created_dtime = None
                log_data_raw_jsonb = None


                if logg['type'] == 'STATUS':
                     
                     logs_status_content = {   
                                            'vacancy_id' : find_value(logg, 'vacancy'),
                                            'applicant_vacancy_status_id' : find_value(logg,'status'),
                                            'applicant_vacancy_rejection_reason_id' : find_value(logg, 'rejection_reason'),
                                            'applicant_vacancy_new_status_created_dtime' : find_value(logg, 'created'),
                                            'log_data_raw_jsonb' : json.dumps(logg ,ensure_ascii=False)
                                        }
                     logggs.append(logs_status_content)
                
            content = {
                'applicant_id': applicant_id,
                'logs' : logggs

            }

            
            insert_fct_applicant_statuses.append((current_time, current_time, dag_id, json.dumps(content)))
        
        cursor.executemany(insert_query_fct_applicant_statuses, insert_fct_applicant_statuses)
        conn.commit()
        log.info('Data has been imported to PostgreSQL successfully.')
        counting_rows_added(fct_applicant_statuses, cursor) 


        #importing table with applicant duplicates 

        pg_creating_table(fct_applicant_duplicates, conn, cursor)
        insert_query_fct_applicant_duplicates = f"INSERT INTO {fct_applicant_duplicates} (row_created_ddtm, dag_started_dttm, dag_id, content) VALUES (%s, %s, %s, %s)"

        logs_duplicates = None
        insert_fct_applicant_duplicates = []

        for l in data:
            applicant_id = int(l['id'])
            logs_duplicates = l['logs']

            logggs = []
            
            for logg in logs_duplicates:
                logs_content = {}


                dupl_log_created_by_user_id = None
                dupl_applicant_id           = None
                dupl_comment_txt            = None
                dupl_log_created_dtime      = None

                if logg['type'] == 'DOUBLE':

                    logs_double_content = {
                                            'dupl_log_created_by_user_id': find_value(logg, 'account'),
                                            'dupl_applicant_id': find_value(logg, 'source'),
                                            'dupl_comment_txt': replace_txt(find_value(logg, 'comment')),
                                            'dupl_log_created_dtime': find_value(logg,'created')
                                        }
                    logggs.append(logs_double_content)

            content = {
                        'applicant_id': applicant_id,
                        'logs' : logggs
                        }
            
            insert_fct_applicant_duplicates.append((current_time, current_time, dag_id, json.dumps(content)))
        
        cursor.executemany(insert_query_fct_applicant_duplicates, insert_fct_applicant_duplicates)
        conn.commit()
        log.info('Data has been imported to PostgreSQL successfully.')
        counting_rows_added(fct_applicant_duplicates, cursor) 
        
        #importing table with applicant logs 

        cursor.execute(f"CREATE TABLE IF NOT EXISTS {fct_applicant_logs} (\
                                                applicant_id text,\
                                                log_type text,\
                                                log_created_dtime text,\
                                                log_data_raw_jsonb text,\
                                                row_created_ddtm text,\
                                                dag_id text,\
                                                dag_started_dttm text,\
                                                upd_dtime text\
                                                )")
        conn.commit()
        log.info(f'Table {fct_applicant_logs} has been created successfully')

        logs_logs = None
        insert_fct_applicant_logs = []

        for l in data:
            applicant_id = int(l['id'])
            logs_logs = l['logs']

            loggss = []

            for logg in logs_logs:

                log_type = None
                log_created_dtime = None
                log_data_raw_jsonb = None

                if logg['type'] is not None:
                    log_logs_content = {
                                        'log_type' : find_value(logg,'type'),
                                        'log_created_dtime' : find_value(logg,'created'),
                                        'log_data_raw_jsonb' : json.dumps(logg,ensure_ascii=False)
                    }
                    loggss.append(log_logs_content)

            content = {
                        'applicant_id'       : applicant_id,
                        'logs'               : loggss,
                        }

            insert_fct_applicant_logs.append((json.dumps(content)))

        metadata_columns  = {'row_created_ddtm' :current_time, 'dag_started_dttm' : current_time, 'dag_id' : dag_id, 'upd_dtime' : current_time}

        for entry in insert_fct_applicant_logs:

            dict_data = json.loads(entry)
            new_df    = pd.DataFrame(dict_data)
            logs_df   = pd.DataFrame(new_df['logs'].tolist())

            logs_df['applicant_id'] = new_df['applicant_id']
            df_all_columns          = logs_df[['applicant_id','log_type', 'log_created_dtime', 'log_data_raw_jsonb']]
            total_df                = df_all_columns.assign(**metadata_columns)

            total_df.to_csv('data.csv', sep=',',columns=[
                                                  'row_created_ddtm'
                                                , 'dag_started_dttm'
                                                , 'dag_id'
                                                , 'applicant_id'
                                                , 'log_type'
                                                , 'log_created_dtime'
                                                , 'log_data_raw_jsonb'
                                                , 'upd_dtime'
                                                ], index=False)

            with open ('data.csv', 'r', newline='', encoding='utf-8') as copyfile:
                copy_sql = f"COPY {fct_applicant_logs} (row_created_ddtm, dag_started_dttm, dag_id, applicant_id, log_type, log_created_dtime, log_data_raw_jsonb, upd_dtime) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'"
                cursor.copy_expert(sql=copy_sql, file=copyfile)
                conn.commit()
            
            os.remove('data.csv')

            del total_df
            del logs_df
        
        del insert_fct_applicant_logs
        gc.collect()
        log.info('Temporary csv chunk has been deleted')   
            
        log.info('Data has been imported to PostgreSQL successfully.')
        counting_rows_added(fct_applicant_logs, cursor)


        #importing table with applicant tags

        pg_creating_table(fct_applicant_tags, conn, cursor)        
        insert_query_fct_applicant_tags = f"INSERT INTO {fct_applicant_tags} (row_created_ddtm, dag_started_dttm, dag_id, content) VALUES (%s, %s, %s, %s)"

        insert_fct_applicant_tags = []

        for l in data:

            applicant_id = int(l['id'])
            tags = l['tags']

            for t in tags:
                t

            content = {
            'applicant_id' : applicant_id,
            'tag_id'       : tags
            }

            insert_fct_applicant_tags.append((current_time, current_time, dag_id, json.dumps(content)))
        
        cursor.executemany(insert_query_fct_applicant_tags, insert_fct_applicant_tags)
        conn.commit()
        log.info('Data has been imported to PostgreSQL successfully.')
        counting_rows_added(fct_applicant_logs, cursor)


        #importing table with applicant resumes

        pg_creating_table(fct_applicant_resumes, conn, cursor) 
        insert_query_fct_applicant_resumes = f"INSERT INTO {fct_applicant_resumes} (row_created_ddtm, dag_started_dttm, dag_id, content) VALUES (%s, %s, %s, %s)"

        insert_fct_applicant_resumes = []
        for l in data:

            applicant_id = int(l['id'])
            externals = l['externals']

            exxt_attr = []

            for ex in externals:
                resume_id = None
                source_id = None
                source_auth_type_nm = None
                source_updated_dttm = None
                source_created_dttm = None

                if ex is not None:
                    ext_content = {
                                    'resume_id' : find_value(ex,'id'),
                                    'account_source' : find_value(ex,'account_source'),
                                    'auth_type' : find_value(ex,'auth_type'),
                                    'updated' : find_value(ex,'updated'),
                                    'created' : find_value(ex,'created')
                    }
                    exxt_attr.append(ext_content)

            content = {
                        'applicant_id' : applicant_id,
                        'externals'    : exxt_attr,

            }

            insert_fct_applicant_resumes.append((current_time, current_time, dag_id, json.dumps(content)))
        
        cursor.executemany(insert_query_fct_applicant_resumes, insert_fct_applicant_resumes)
        conn.commit()
        log.info('Data has been imported to PostgreSQL successfully.')
        counting_rows_added(fct_applicant_logs, cursor)


    del data_gen
    gc.collect()

    cursor.close()
    conn.close()
    log.info(f'JSON has been entirely inserted succesfully.')



def read_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()
    
def execute_query_from_stg_directory(directory, table_name):

    try:
        file_list_stg = os.listdir(os.path.join(directory, 'hfarchive/', 'stg/'))

        for file_name in file_list_stg:

            if table_name in file_name and file_name.endswith(".sql"):

                file_path = os.path.join(directory, 'hfarchive/', 'stg/', file_name)
                sql_query = read_file(file_path)
                log.info('SQL file for stg-layer was read successfully')
                log.info(f'SQL script content:\n{sql_query}')

                return sql_query

        log.info("SQL-query file executed successfully")

    except Exception as e:
        log.error(f'Error executing query: {str(e)}')
        raise AirflowException('SQL error')

def execute_query_from_ods_directory(directory, table_name):

    try:

        file_list_stg = os.listdir(os.path.join(directory, 'hfarchive/', 'ods/'))

        for file_name in file_list_stg:

            if table_name in file_name and file_name.endswith(".sql"):

                file_path = os.path.join(directory, 'hfarchive/', 'ods/', file_name)
                sql_query = read_file(file_path)
                log.info('SQL file for ods-layer was read successfully')
                log.info(f'SQL script content:\n{sql_query}')

                return sql_query

        log.info("SQL-query file executed successfully")

    except Exception as e:
        log.error(f'Error executing query: {str(e)}')
        raise AirflowException('SQL error')

def stg_etl(directory, table_name):

    hook = PostgresHook(postgres_conn_id="pg-conn")
    log.info('Initializing hook')
    conn = hook.get_conn()
    log.info('Hook initialized')
    cursor = conn.cursor()

    sql_query_stg = execute_query_from_stg_directory(directory, table_name)

    try:
        cursor.execute(sql_query_stg)
        conn.commit()
        log.info('Data has been successfully transefred into stg-layer')

    except Exception as e:
        log.error(f'Error executing query: {str(e)}')
        conn.rollback()
        raise AirflowException('SQL error')

    finally:
        cursor.close()
        conn.close()

def ods_etl(directory, table_name):

    hook = PostgresHook(postgres_conn_id="pg-conn")
    log.info('Initializing hook')
    conn = hook.get_conn()
    log.info('Hook initialized')
    cursor = conn.cursor()

    sql_query_ods = execute_query_from_ods_directory(directory, table_name)

    try:
        cursor.execute(sql_query_ods)
        conn.commit()
        log.info('Data has been successfully transefred into ods-layer')

    except Exception as e:
        log.error(f'Error executing query: {str(e)}')
        conn.rollback()
        raise AirflowException('SQL error')       

    finally:
        cursor.close()
        conn.close()


with DAG(
        **globals.DEFAULT_DAG_CONFIG,
        dag_id           = 'hfarchive_etl',
        description      = 'Load applicants data from Harchive data to DWH and process it',
        schedule_interval='0 18 * * *',
        start_date       = datetime(2022, 8, 17),
        # dagrun_timeout   = timedelta(minutes=30),
        tags             = ['Harchive'],
        concurrency      = 2
) as dag:

    DAGS_PATH  = airflow_settings.DAGS_FOLDER
    LOAD_PATH  = '/tmp/p'
    UNZIP_PATH = f'{LOAD_PATH}/export'
    FILE_NAME  = 'applicants'
    CHUNK_SIZE = 50000
    if IS_ENV_DEV:
        DAGS_PATH = os.getcwd()

    pwd = VariableGetter('pas')

    start_dag  = DummyOperator(task_id='start_dag')
    finish_dag = DummyOperator(task_id='finish_dag')

    start_load_file  = DummyOperator(task_id='start_load_file')
    finish_load_file = DummyOperator(task_id='finish_load_file')

    start_etl  = DummyOperator(task_id='start_etl')
    finish_etl = DummyOperator(task_id='finish_etl')

    start_dag >> start_load_file

    #load and unzip file

    mkdir_tmp = BashOperator(
        task_id='mkdir_tmp',
        bash_command=f'mkdir -p {UNZIP_PATH}'
    )

    load_to_tmp = BashOperator(
        task_id='load_file_to_tmp',
        bash_command=f'to zip'
    )

    unzip_file = PythonOperator(
        task_id='unzip_file',
        python_callable=unzip,
        op_kwargs={
            'zip_path': f'{LOAD_PATH}/export.zip',
            'zip_to_path': UNZIP_PATH,
            'pwd': pwd
        })

    import_task = PythonOperator(
                                task_id="import_json_task",
                                python_callable=import_json_to_postgres,
                                provide_context=True,
                                op_kwargs={'data': {
                                                    'path_to_folder': f"{UNZIP_PATH}/"
                                                    , 'file_name': f"{FILE_NAME}" 
                                                    , 'chunk_size': CHUNK_SIZE
                                                    },
                                        'table_names': [
                                                          dct_applicants
                                                        , fct_applicant_vacancies
                                                        , fct_applicant_statuses
                                                        , fct_applicant_logs
                                                        , fct_applicant_tags
                                                        , fct_applicant_resumes    
                                                        , fct_applicant_tags
                                                    ]       
                                        },
                                dag=dag)

    start_load_file >> mkdir_tmp >> load_to_tmp >> unzip_file >> finish_load_file
    finish_load_file >> import_task >> finish_etl >> finish_dag