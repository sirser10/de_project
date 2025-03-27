import logging
from airflow.models.baseoperator import BaseOperator

from hooks.teams_api_hook import TeamsAPIHook

log = logging.getLogger(__name__)

class TeamsAPIOperator(BaseOperator):

    def __init__(self,
                 task_id: str,
                 conn_id: str,
                 task_key: str = None,
                 task_kwargs: dict = {},
                 **kwargs):

        super().__init__(
            task_id=task_id,
            **kwargs
        )

        self.conn_id = conn_id
        self.task_key = task_key if task_key is not None else task_id
        self.task_kwargs = task_kwargs

    def send_message(self):
        self.hook.send_message(**self.task_kwargs)
        log.info(f'Successfully send message to chat_id={str(self.task_kwargs["chat_id"])}')
        return True

    def execute(self, context):
        self.hook = TeamsAPIHook(conn_id=self.conn_id)
        return getattr(self, self.task_key)()

