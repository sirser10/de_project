import logging
import warnings
import os

from manatal_project.operators.pg_operator import CustomPGOperator

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)


class SQLScriptExecution(CustomPGOperator):

    def __init__(
                self,
                pg_hook_con: str,
                read_sql_cfg: dict,
                task_id=None,
                task_key='sql_script_execution', 
                **kwargs
                ) -> None:
        
        super().__init__(
                        pg_hook_con=pg_hook_con,
                        task_id=task_id,
                        task_key=task_key,
                        **kwargs
                        )

        self.directory     = read_sql_cfg['directory']
        self.schema_folder = read_sql_cfg.get('schema_folder', None)
        self.table_name    = read_sql_cfg['table_name']

        self.exec_method   = {'sql_script_execution' : self.sql_script_execution}


    def execute(self, context):
        return super().execute(context)

    def sql_script_execution(self) -> None:

        sql_query = self.read_sql_query(
                                directory=self.directory,
                                schema=self.schema_folder,
                                table_name=self.table_name
                                )
        
        try:
            self.cursor.execute(sql_query)
            table_ = self.schema_folder + '.' + self.table_name if self.schema_folder else self.table_name
            if 'inserted_rows' in sql_query:
                inserted_rows = self.cursor.fetchone()[0]
                log.info(f"{table_} has been inserted {inserted_rows} rows")

            log.info(f'Table {table_} has been processed successfully.')    

            if '.' in self.table_name and self.schema_folder is not None:
                schema_folder, table_name = self.schema_folder, self.table_name.split('.')[1]
            elif '.' in self.table_name and self.schema_folder is None:
                schema_folder, table_name = self.table_name.split('.')[0], self.table_name.split('.')[1]
            else:
                schema_folder, table_name = self.schema_folder, self.table_name


            self.count_table_rows(schema=schema_folder, table_name=table_name)
            self.conn.commit()

        except Exception as e:
            log.info("Error:", e)
            self.conn.rollback()
            raise 

        finally:
            self.cursor.close()
            self.conn.close()
        
    def read_sql_query(
                        self,
                        directory: str,
                        table_name: str,
                        schema: str=None,
                        ) -> None:
        
        def read_file(file_path) -> None:
            with open(file_path, 'r') as file:
                return file.read()

        try:
            log.info(f'Start processing sql-files in directory: {directory}')
            if not schema:
                log.info('Processing wo schema-folder')
                file_list: list = os.listdir(directory)
                for file_name in file_list:
                    file_name_wo_sch = file_name.split('.')[1] + '.sql' if '.' in file_name and len(file_name.split('.')) > 2 else file_name
                    if table_name.split('.')[1] == file_name_wo_sch.split('.')[0] and file_name.endswith(".sql"):
                        file_path: str = os.path.join(directory, file_name)
                        log.info(f'Revelant file path is {file_path}')
                        sql_query = read_file(file_path)
                        log.info(f'SQL script content:\n{sql_query}')

                        return sql_query

            else:
                log.info('Processing with schema-folder')
                file_list: list = os.listdir(os.path.join(directory, schema))
                for file_name in file_list:
                    file_name_wo_sch = file_name.split('.')[1] + '.sql' if '.' in file_name and len(file_name.split('.')) > 2 else file_name
                    if table_name == file_name_wo_sch.split('.')[0] and file_name.endswith(".sql"):
                        file_path: str = os.path.join(directory, schema, file_name)
                        log.info(f'Revelant file path is {file_path}')
                        sql_query = read_file(file_path)
                        log.info(f'SQL file for {schema}-schema was read successfully')
                        log.info(f'SQL script content:\n{sql_query}')

                        return sql_query
                
            log.info("SQL-query file executed successfully")

        except Exception as e:
            log.info(f'Error: {e}')
            raise