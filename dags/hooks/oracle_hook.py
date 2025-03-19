from airflow.models.variable import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook

import logging
log = logging.getLogger(__name__)

class CustomOracleHook(OracleHook):

    def __init__(self, env: str, **kwargs)->None:
        
        self.env=env

        if self.env == 'PROD':
            self.conn_id: str = Variable.get('oracle_conn_id')
        elif self.env == 'DEV':
            self.conn_id: str = Variable.get('oracle_dev_conn_id')
        else:
            raise ValueError(f"Invalid environment: {self.env}")

        super().__init__(oracle_conn_id=self.conn_id, **kwargs)
    
    def oracle_hook_conn(self):
        return super().get_conn()