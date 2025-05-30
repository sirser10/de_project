import datetime
from datetime import timedelta, datetime

current_date: str           = datetime.now().date()
yesterday_date: str         = (current_date- timedelta(days=1)).strftime("%Y-%m-%d")

EMPLOYEE_SAP_CFG: dict   =  {
}

LEGAL_STRUCT_RENAME_COLUMN =\
    {
}