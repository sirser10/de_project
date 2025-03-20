from typing import(
                    Any,
                    Dict,
                    Union
                    ) 
from datetime import datetime as dttm

import re
import logging
import warnings

log = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

class CommonHelperOperator:

    def __init__(
                self,
                s3_obj_path: str = None,
                file_name: str = None,
                file_name_mapping: Dict[str,str] = None,
                **kwargs
                ) -> None:
        
        self.s3_obj_path       = s3_obj_path
        self.file_name         = file_name
        self.file_name_mapping = file_name_mapping

    def get_metadata_columns(
                            self,
                            s3_obj_path: str=None,
                            context={},
                            ) -> Dict[str, Union[str, dttm]]:
        
        metadata_columns_res: Dict[str, Union[str, dttm]] = {
                                                'row_created_dttm' : dttm.now(),
                                                **context,
                                                **({'obj_path':s3_obj_path} if s3_obj_path is not None else {}),
                                                'updated_dttm'        : dttm.now(),
                                                }
        return metadata_columns_res

    def get_file_name_from_config(self, file_name: str, file_name_mapping: dict) -> str:

        for key, value in file_name_mapping.items():
            if key in file_name:
                return value
        return file_name

    def get_file_name_wo_ext(self, file_name: str) -> str:

        file_name_full: str   = file_name.split('/')[-1]
        file_name_wo_ext: str = '.'.join(file_name_full.split('.')[:-1])

        return file_name_wo_ext


class DateGetterHelper:
    
    def __init__(
                self,
                date_edge_cfg: dict[str, Any] = {},
                str_obj: str = None,
                **kwargs
                ) -> None:
    
        self.str_obj       = str_obj
        self.date_edge_cfg  = date_edge_cfg

    def date_detector(self, str_obj: str) -> bool:

        date_pattern = r'^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$|^\d{1,2} \w+ \d{4}$|^\d{2}\.\d{2}\.\d{4}$'
        
        if re.match(date_pattern, str_obj):
            date_formats = [
                            '%Y-%m-%d',      
                            '%d/%m/%Y',     
                            '%m/%d/%Y',    
                            '%d %B %Y',     
                            '%B %d, %Y',     
                            '%d %b %Y',
                            '%d.%m.%Y'     
                        ]
            
            for v in date_formats:
                try:
                    r_ = dttm.strptime(str_obj, v)
                    return True 
                except ValueError:
                    continue
        return False
    
    def datetime_detector(self, str_obj: str) -> bool:
        
        datetime_pattern: str = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[Za-zA-Z]$'
        if re.match(datetime_pattern, str_obj):
            dttm_f = [
                    '%Y-%m-%dT%H:%M:%SZ', #ISO 8601
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M',
                    '%H:%M',
                    '%H:%M:%S',
                    ]
            for v in dttm_f:
                try:
                    r_ =dttm.strptime(str_obj, v)
                    return True
                except ValueError:
                    continue
        return False
