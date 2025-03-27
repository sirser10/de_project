import logging
import warnings
from typing import Any
from operators.common.pg_operator import CustomPGOperator

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

class PostgresEntityBackupExecutor(CustomPGOperator):
    """
    Класс позволяет создать back-up целевой сущности.
    Также класс при отсутствии создает bckp схему в Postgres, где будут находиться бэкапы сущностей.
    """
    def __init__(
                self, 
                table_name: str, 
                schema_from: str=None,
                schema_to: str='bckp',
                task_id: str=None,
                task_key: str=None,
                **kwargs
                ) -> None:
        
        if task_id is not None:
            super().__init__(task_id=task_id,**kwargs)

        self.task_key = task_key if task_key is not None else task_id

        self.schema_from   = schema_from
        self.table_name    = table_name
        self.schema_to     = schema_to

        self.exec_method = {'backup_table_process': self.backup_table_process}
        self.dag_context = {key: None for key in ['dag_id', 'dag_run_id', 'dag_start_dttm']}

    def execute(self, context: dict) ->Any:
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
    
    def backup_table_process(self) -> None:
    
        log.info(f'BACKUP process has been launched.')
        try:
            schema_existanse_check_query = f"""
                                            SELECT EXISTS (
                                                            SELECT 1 
                                                            FROM pg_catalog.pg_namespace 
                                                            WHERE nspname = '{self.schema_to}'
                                            );
                                            """
            log.info(schema_existanse_check_query)
            self.cursor.execute(schema_existanse_check_query)
            schema_exists = self.cursor.fetchone()[0]
            log.info(f"Schema exists status - {schema_exists}")

            if schema_exists:
                log.info(f'Schema {self.schema_to} has already existed.')
                pass
            else:
                log.info(f"Schema {self.schema_to} hasn't been found in database. Completing CREATE SCHEMA...")
                self.create_bckp_schema()

            #bckp-table creation process
            bckp_table_name = f'{self.schema_from}_{self.table_name}' if self.schema_from else self.table_name.replace('.','_')
            schema_from , table_from_name = self.table_name.split('.')
            info_schema_query: str = f"""
                                        SELECT EXISTS (
                                                        SELECT 1 
                                                        FROM information_schema.tables 
                                                        WHERE table_schema = '{self.schema_to}' AND table_name = '{bckp_table_name}'
                                                        )
                                        ;
                                    """
            self.cursor.execute(info_schema_query)
            log.info(info_schema_query)
            table_exists = self.cursor.fetchone()[0]
            log.info(f"Table exists status - {table_exists}")

            back_up_query = f"""
                                    DROP TABLE IF EXISTS {self.schema_to}.{bckp_table_name};
                                    CREATE TABLE IF NOT EXISTS {self.schema_to}.{bckp_table_name} AS 
                                    SELECT
                                            target.*
                                            , '{self.dag_context['dag_id']}':: TEXT AS dag_id
                                            , '{self.dag_context['dag_run_id']}':: TEXT AS dag_run_id
                                            , '{self.dag_context['dag_start_dttm']}':: TEXT as dag_start_dttm
                                            , current_timestamp as bckp_updated_dttm 
                                    FROM {schema_from}.{table_from_name} target
                                    ;
                                """
            if table_exists:
                log.info(f"Table {self.schema_to}.{bckp_table_name} exists. Launching updating backup table processes...")
                log.info(back_up_query)
                self.cursor.execute(back_up_query)    
                self.conn.commit()
                log.info(f"The backup of {schema_from}.{table_from_name} into {self.schema_to} completed successfully.")

            else:
                log.info(f"Table {self.schema_to}.{bckp_table_name} exists. Launching creating backup table process...")
                log.info(back_up_query)
                self.cursor.execute(back_up_query)
                self.conn.commit()
                log.info(f"The backup of {schema_from}.{table_from_name} into {self.schema_to} completed successfully.")

            count_rows_query: str = f"""SELECT COUNT(*) FROM %s.%s;"""
            self.cursor.execute(count_rows_query % (schema_from, table_from_name))
            count_rows_from_res = self.cursor.fetchone()[0]
                        
            self.cursor.execute(count_rows_query % (self.schema_to, bckp_table_name))
            count_rows_to_res = self.cursor.fetchone()[0]

            if count_rows_from_res != count_rows_to_res:
                log.info(f"The rows number of both tables is different: {count_rows_from_res}!={count_rows_to_res}")
                raise Exception('Different rows number')
            else:
                log.info(f"The rows number of both tables are same: {count_rows_from_res}={count_rows_to_res}")
        
        except Exception as e:
            log.info("Error:", e)
            self.conn.rollback()
            raise
        finally:
            self.cursor.close()
            self.conn.close()

    def create_bckp_schema(self) -> None:

        create_bckp_schema_query = f"CREATE SCHEMA {self.schema_to};"
        log.info(create_bckp_schema_query)

        self.cursor.execute(create_bckp_schema_query)
        log.info(f"Schema exists status - {self.schema_to}")
        log.info(f"Schema {self.schema_to} has been successfully created.")
