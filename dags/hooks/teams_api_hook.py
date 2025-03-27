import os
import logging

from airflow.hooks.base import BaseHook
from bot.bot import Bot

log = logging.getLogger(__name__)

IS_ENV_DEV = False

if 'IS_ENV_DEV' in os.environ:
    IS_ENV_DEV = bool(os.environ.get('IS_ENV_DEV'))
    if IS_ENV_DEV:
        log.info('Starting with dev environment')

if IS_ENV_DEV:
    from globals.secret import teams
    class DEFAULTS:
        conns = {
            'vkteams-api-hran-bot': {
                'server': 'https://some_api.server.teams.ru/bot/v1',
                'token': teams.token
            }
        }


class TeamsAPIHook(BaseHook):

    def __init__(self, conn_id: str, *args, **kwargs):
        log.info('Hook init')

        if not IS_ENV_DEV:
            log.info('Calling BaseHook from prod')
            super().__init__(*args, **kwargs)
            log.info('Called BaseHook from prod')

        log.info('Getting connection')
        self.conn = self.get_connection(conn_id=conn_id)
        log.info('Connection successful')

        self.kwargs = kwargs

    def get_connection(self, conn_id: str):

        if IS_ENV_DEV:

            _creds = DEFAULTS.conns[conn_id]
            _cfg = {
                'server': _creds['server'],
                'token': _creds['token']
            }


        if not IS_ENV_DEV:
            _creds = super().get_connection(conn_id=conn_id)

            _cfg = {
                'server': _creds.host,
                'token': _creds.password
            }

        self.Bot = Bot(token=_cfg['token'], api_url_base=_cfg['server'], is_myteam=True)
        return self

    def send_message(self, chat_id: int, msg_text: str, **kwargs):
        return self.Bot.send_text(chat_id=chat_id, text=msg_text, **kwargs)


