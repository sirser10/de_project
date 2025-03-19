import logging
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

class CustomS3Hook(S3Hook):

    def __init__(self, env: str)->None:
        
        self.env=env

        if self.env == 'DEV':
            self.conn_id = Variable.get('corpcloud_conn_id')
        elif self.env == 'PROD':
            self.conn_id = Variable.get('prodcloud_conn_id')
        else:
            raise ValueError(f"Invalid environment: {self.env}")
        
        super().__init__(aws_conn_id=self.conn_id)

    
    def s3_hook_conn(self):
        return super().get_conn()