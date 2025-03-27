from pandas import DataFrame
from typing import Literal, Union, Any
from io import TextIOWrapper
from datetime import datetime as dttm, timedelta

from mytracker_export_api import MyTracker
from operators.common.s3_operator import S3CustomOperator
from airflow.models.baseoperator import BaseOperator 

import warnings
import logging
import pandas as pd
import os

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)


class MyTrackerOperator(BaseOperator):
    
    def __init__(
                self,
                task_id: str,
                pass_config: dict,
                report_params: dict,
                s3_ingest_config: dict, 
                date_edged_params: dict = {},
                load_type: Literal['full', 'incremental']='incremental', 
                task_key: str=None,
                **kwargs
                ) -> None:
        
        super().__init__(task_id=task_id, **kwargs)
        
        self.pass_config       = pass_config
        self.report_params     = report_params
        self.s3_ingest_config  = s3_ingest_config
        self.date_edged_params = date_edged_params
        self.load_type         = load_type
        self.date_edge_cfg     = date_edged_params
        
        self.client      = MyTracker(**self.pass_config)
        self.s3_operator = S3CustomOperator(
                                            s3_hook_con=self.s3_ingest_config['s3_hook_con'],
                                            bucket=self.s3_ingest_config['bucket'],
                                            )
        
        self.task_key = task_key
        self.exec_method: dict = {
            'get_mytracker_raw_data': self.get_mytracker_raw_data,
            'get_mytracker_reports': self.get_mytracker_reports,
        }
        
    def execute(self, context: Any) -> Any:
        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res
        
    def get_mytracker_raw_data(self) -> None:

        log.info('Launched fetching RAW DATA from MyTracker...')
        if self.load_type == 'incremental':

            log.info(f'Type of data loading: {self.load_type}')
            date_from, date_to = self.get_date_edges_for_mytracker()
            
            for key, value in self.report_params.items():

                log.info(f"The report {key} has been created for {date_from} and {date_to}")
                value['dateFrom'], value['dateTo'] = date_from, date_to

                if key.startswith('dim_banner'):
                    report: DataFrame = self.client.get_raw_data(value)
                    report_values = list(map(lambda row: eval(row), report.to_dict()['params.value'].values()))
                    
                    mapping = {}
                    for index, row in enumerate(report_values):
                        if row[0] not in mapping:
                            mapping[row[0]] = {row[1]: 1}
                        else:
                            mapping[row[0]][row[1]] = mapping[row[0]][row[1]] + 1 if row[1] in mapping[row[0]] else 1
                    
                    banner_mapping = {}
                    for key_ in mapping:
                        value = mapping[key_]
                        banner_mapping[key_] = list(value.keys())[0]

                    df_banner = pd.DataFrame(list(banner_mapping.items()), columns=['banner_id', 'banner_name'])
                    self.s3_operator.s3_ingest_loader(
                                                    ingest_path=self.s3_ingest_config['ingest_path'],
                                                    obj_to_load=df_banner,
                                                    file_name=key
                                                    )

                elif key.startswith('user_custom_events'):
                    df_user_custom_events_raw_report: DataFrame = self.client.get_raw_data(value)
                    df_raw_report: DataFrame = self.custom_events_df_normilizer(df_user_custom_events_raw_report, 'user_custom_events')
                    self.s3_operator.s3_ingest_loader(
                                                    ingest_path=self.s3_ingest_config['ingest_path'],
                                                    obj_to_load=df_raw_report,
                                                    file_name=key
                                                    )
                else:
                    df_raw_report: DataFrame = self.client.get_raw_data(value)
                    self.s3_operator.s3_ingest_loader(
                                                    ingest_path=self.s3_ingest_config['ingest_path'],
                                                    obj_to_load=df_raw_report,
                                                    file_name=key
                                                    )

        else:
            log.info(f'Type of data loading: {self.load_type}')
            log.info(f'Start getting RAW DATA from MyTracker.')

            #erase dim-reports from reports due to the given conditions 
            raw_data_params= {k:self.report_params[k] for k in self.report_params if k != 'dim_banner'}
            
            for key, value in raw_data_params.items():
                log.info(f'{key} report processing...')

                raw_empty_dataframe = pd.DataFrame()
                date_params= self.date_edged_params.copy()
                edge_month = 1
                st_date, end_dt, limit_edge = self.get_date_edges_for_mytracker(**date_params)
                current_date: str = dttm.today().date().strftime('%Y-%m-%d')

                while edge_month <= limit_edge and end_dt <= current_date:
                    date_params['start_month'] = edge_month % 12 or 12
                    date_params['start_year'] += edge_month // 13

                    value['dateFrom'], value['dateTo'], _ = self.get_date_edges_for_mytracker(**date_params)
                    log.info(f"Partition dates: {value['dateFrom']}, {value['dateTo']}")

                    if key.startswith('user_custom_events'):

                        df_user_custom_events_raw_report: DataFrame = self.client.get_raw_data(value)
                        df_raw_report: DataFrame = self.custom_events_df_normilizer(df_user_custom_events_raw_report, 'user_custom_events')
                    else:
                        df_raw_report: DataFrame = self.client.get_raw_data(value)
                    
                    raw_empty_dataframe = pd.concat([raw_empty_dataframe,df_raw_report], ignore_index=True)
                    edge_month +=1

                value['dateFrom'], value['dateTo'] = (dttm.strptime(value['dateTo'], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'), current_date
                log.info(f"Partition dates: {value['dateFrom']}, {value['dateTo']}")

                if key.startswith('user_custom_events'):
                    df_user_custom_events_raw_report: DataFrame = self.client.get_raw_data(value)
                    df_raw_report: DataFrame = self.custom_events_df_normilizer(df_user_custom_events_raw_report, 'user_custom_events')
                else:
                    df_raw_report: DataFrame = self.client.get_raw_data(value)
                
                raw_empty_dataframe = pd.concat([raw_empty_dataframe,df_raw_report], ignore_index=True)
                log.info(f"{key} DataFrame has the next dates: {pd.to_datetime(raw_empty_dataframe['tsEvent'], unit='s').dt.to_period('M').unique()}")
                self.s3_operator.s3_ingest_loader(
                                                ingest_path=self.s3_ingest_config['ingest_path'],
                                                obj_to_load=raw_empty_dataframe,
                                                file_name=key
                                                )
    
    def get_mytracker_reports(self) -> None:

        log.info('Launched fetching data from MyTracker...')
       
        if self.load_type == 'incremental':

            log.info(f'Start getting MAIN REPORTS from MyTracker')
            log.info(f'Type of data loading: {self.load_type}')
            date_from, date_to = self.get_date_edges_for_mytracker()

            for key, value in self.report_params.items():

                log.info(f"The report {key} has been created for {date_from} and {date_to}")
                value['settings[filter][date][0][from]'], value['settings[filter][date][0][to]'] = date_from, date_to
                df_report: DataFrame = self.client.get_report(value)
                self.s3_operator.s3_ingest_loader(
                                                ingest_path=self.s3_ingest_config['ingest_path'],
                                                obj_to_load=df_report,
                                                file_name=key
                                                )

            log.info(f'End getting data from MAIN REPORTS MyTracker')
            log.info('Process of ingesting data from MyTracker completed successfully')

        else:
            log.info(f'Start getting MAIN REPORTS from MyTracker')
            log.info(f'Type of data loading: {self.load_type}')

            for key, value in self.report_params.items():
                log.info(f'{key} report processing...')

                empty_dataframe = pd.DataFrame()
                date_params= self.date_edged_params.copy()
                edge_month = 1
                st_date, end_dt, limit_edge = self.get_date_edges_for_mytracker(**date_params)
                current_date: str = dttm.today().date().strftime('%Y-%m-%d')

                while edge_month <= limit_edge and end_dt <= current_date:
                    if edge_month == 7 and key in {'pages_activity_daily', 'pages_activity_monthly'} and date_params['start_year'] == 2024:
                        edge_month += 1; continue
                    
                    date_params['start_month'] = edge_month % 12 or 12 
                    date_params['start_year'] += edge_month // 13

                    value['settings[filter][date][0][from]'], value['settings[filter][date][0][to]'], _ = self.get_date_edges_for_mytracker(**date_params)
                    log.info(f"Partition dates: {value['settings[filter][date][0][from]']}, {value['settings[filter][date][0][to]']}")

                    df_report: DataFrame = self.client.get_report(value)
                    empty_dataframe = pd.concat([empty_dataframe,df_report], ignore_index=True)
                    edge_month +=1

                value['settings[filter][date][0][from]'], value['settings[filter][date][0][to]'] = (dttm.strptime(value['settings[filter][date][0][to]'], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'), current_date
                log.info(f"Partition dates: {value['settings[filter][date][0][from]']}, {value['settings[filter][date][0][to]']}")
                df_report: DataFrame = self.client.get_report(value)
                total_df: DataFrame = pd.concat([empty_dataframe,df_report], ignore_index=True)
                log.info(f"{key} DataFrame contains the next dates:"
                         f"{pd.to_datetime(total_df['Date']).dt.to_period('M').unique() if 'Date' in list(total_df.columns) else pd.to_datetime(total_df['Month']).dt.to_period('M').unique()}\
                         ")
                self.s3_operator.s3_ingest_loader(
                                                ingest_path=self.s3_ingest_config['ingest_path'],
                                                obj_to_load=total_df,
                                                file_name=key
                                                )
            log.info(f'End getting data from MAIN REPORTS MyTracker')
            log.info('Process of ingesting data from MyTracker completed successfully')


    def get_date_edges_for_mytracker(
                                self,
                                start_date: str=None,
                                end_date: str=None,
                                increment_type: Literal['annually', 'daily'] = 'daily', 
                                start_year: int=None,
                                start_month: int=1
                                ) -> Union[tuple[str, str], tuple[str,str,int]]:

        from dateutil.relativedelta import relativedelta

        requested_date: dttm = dttm.today().date() if not start_year else dttm.today().date().replace(year=start_year)
        if all([not start_date, not end_date, not start_year]):
            start_date_verge = (requested_date - timedelta(days=1)).strftime('%Y-%m-%d')
            end_date_verge = requested_date.strftime('%Y-%m-%d')

        elif start_year and increment_type == 'annually':

            start_year_date, end_year_date , current_date = dttm(requested_date.year,1,1).date(), dttm(requested_date.year,12,31).date() , dttm.today().date()
            start_date_verge: str = (start_year_date + relativedelta(month=start_month)).strftime('%Y-%m-%d')
            end_date_verge: str   = (end_year_date + relativedelta(month=start_month)).strftime('%Y-%m-%d')
            edge_step: int = (current_date.year - start_year_date.year) * 12 + (current_date.month - start_year_date.month)

        return (start_date_verge, end_date_verge) + ((edge_step,) if increment_type != 'daily' else ())

    def custom_events_df_normilizer(self, custom_events_df: DataFrame, buff_f_name: str) -> DataFrame:

        csv_buff_name = buff_f_name + '.csv'
        report_dict: dict = custom_events_df.to_dict('list')
        file: TextIOWrapper = open(csv_buff_name, 'w')
        file.write('customUserId,tsEvent,idAppTitle,eventName,params.name,params.value\n')
        for user_id, ts_event, app_title, event_name, params_name_list_str, params_value_list_str in zip(*report_dict.values()):
            params_name_list = eval(params_name_list_str)
            params_value_list = eval(params_value_list_str)
            for param_name, param_value in zip(params_name_list, params_value_list):
                file.write(
                        f'{user_id}, {ts_event}, {app_title}, {event_name}, {param_name}, {param_value}\n'
                            )
        file.close()
        custom_events_df: DataFrame = pd.read_csv(csv_buff_name)
        os.remove(csv_buff_name)

        return custom_events_df