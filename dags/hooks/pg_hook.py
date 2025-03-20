from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging

log = logging.getLogger(__name__)

class CustomPGHook(PostgresHook):
    
    def __init__(self, env: str) -> None:
        
        self.env = env

        if self.env == 'DEV':
            self.conn_id = Variable.get('pg_dev_conn')
        elif self.env == 'PROD':
            self.conn_id = Variable.get('pg_prod_conn')
        else:
            raise ValueError(f"Invalid environment: {env}")
        
        super().__init__(postgres_conn_id=self.conn_id)

    def pg_hook_conn(self):
        try:
            return super().get_conn()
        except Exception as e:
            log.error(f"Error getting PostgreSQL connection: {e}")
            raise e

        



