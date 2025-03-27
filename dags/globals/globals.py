from airflow.models.variable import Variable
from operators.teams_api_operator import TeamsAPIOperator


def on_failure_callback(ctx):

    email_list = Variable.get('failure_emails_list')
    email_list = email_list.split(';')

    ti = ctx['task_instance']

    msg = f'Ошибка в выполнении DAGа {ti.dag_id}. Таск {ti.task_id}'

    for e in email_list:
        TeamsAPIOperator(
            task_id='send_message',
            conn_id='teams-api-bot',
            task_kwargs= {
                'chat_id': e,
                'msg_text': msg
            }
        ).execute({})

    return True

DEFAULT_DAG_CONFIG = {
    'default_args': {
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'depends_on_past': False,
        'on_failure_callback': on_failure_callback
    },
    'catchup': False,
    'max_active_runs': 1
}