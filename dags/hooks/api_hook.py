from requests import Response, HTTPError
from airflow.hooks.base_hook import BaseHook
from typing import Any

import logging
import requests as r
log = logging.getLogger(__name__)

class CustomAPIHook(BaseHook):
      """
      The hook represent universal approach to connect using REST API methods.
      Inheriting Airflow Base Hook, CustomAPIHook requiere only conn_id in most cases.
      After CustomAPIHook init we can apply get_testapi_response method
      """
      def __init__(self, conn_id: str, *args, **kwargs):
                  super().__init__(*args,**kwargs)
                  self.conn_id = conn_id
                  self.connection = None

      def get_custom_connection(self):
            if self.connection is None:
                  self.connection = self.get_connection(self.conn_id)
            return self.connection
     
      def get_restapi_response(
                               self,
                               url: str=None,
                               endpoint: str=None,
                               method: str=None,
                               headers: dict=None,
                               auth: bool=False,
                               params: dict=None,
                               data: Any=None,
                               json: dict=None,
                               **kwargs
                              ) -> Response:
            
            if endpoint is None and url is None:
                   raise ValueError("Either 'endpoint' or 'url' must be provided.")
            if endpoint is not None and url is not None:
                   raise ValueError("Only one of 'endpoint' or 'url' should be provided.")
            if endpoint is not None:
                   url = self.construct_url(endpoint, params)
            if auth:
                  conn = self.get_custom_connection()
                  auth=(conn.login, conn.password)

            method: str = method.upper() or 'GET'

            with r.Session() as session:
                  response: Response = session.request(
                                                      method=method,
                                                      url=url,
                                                      headers=headers,
                                                      auth=auth,
                                                      data=data,
                                                      json=json,
                                                      )  

                  log.info(f"Method: {method}")
                  log.info(f"Url: {url}")
                  log.info(f"Headers: {headers}")
                  log.info(f"Data: {data}") 

                  log.info('Https connection has been complited successfully.') if response.status_code == 200 else response.raise_for_status()

            return response


      def construct_url(self,endpoint: str, params_config: dict) -> str:
            
            conn = self.get_custom_connection()
            host = conn.host
            login = conn.login
            password = conn.password
            auth_type = conn.schema

            params_config = params_config or {}
            if auth_type == 'api_app':
                  params_config.setdefault('api_app', login)
                  params_config.setdefault('api_key', password)
            
            if not host:
                  raise ValueError("Host is not specified in the connection")
            
            url_conf_values: str = "".join(str(value) for value in params_config.values())
            url: str = f"{host}/{endpoint}{url_conf_values}"

            return url