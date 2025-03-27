MYTRACKER_REPORT_PARAMS = {

    'reports' : {
                'dau_daily' :  {
                                        'settings[filter][date][0][from]': '%s',
                                        'settings[filter][date][0][to]': '%s',
                                        'settings[filter][dimension][isVerified][value][]': [255, 1],
                                        'settings[idCurrency]': 840,
                                        'settings[tz]': 'Europe/Moscow',
                                        'settings[precision]': 2,
                                        'settings[retIndent]': 3600,
                                        'settings[selectors][]': ['userDau', 'date'],
                    },
                    'dau_monthly': {
                                    'settings[filter][date][0][from]': '%s',
                                    'settings[filter][date][0][to]': '%s',
                                    'settings[filter][dimension][isVerified][value][]': [255, 1],
                                    'settings[idCurrency]': 840,
                                    'settings[tz]': 'Europe/Moscow',
                                    'settings[precision]': 2,
                                    'settings[retIndent]': 3600,
                                    'settings[selectors][]': ['userDau', 'month'],
                    },
                    'mau': {
                            'settings[filter][date][0][from]': '%s',
                            'settings[filter][date][0][to]': '%s',
                            'settings[filter][dimension][isVerified][value][]': [255, 1],
                            'settings[idCurrency]': 840,
                            'settings[tz]': 'Europe/Moscow',
                            'settings[precision]': 2,
                            'settings[retIndent]': 3600,
                            'settings[selectors][]': ['month', 'userMau'],
                    },
                    'avg_session_duration_monthly': {
                                                    'settings[filter][date][0][from]': '%s',
                                                    'settings[filter][date][0][to]': '%s',
                                                    'settings[filter][dimension][isVerified][value][]': [255, 1],
                                                    'settings[idCurrency]': 840,
                                                    'settings[tz]': 'Europe/Moscow',
                                                    'settings[precision]': 2,
                                                    'settings[retIndent]': 3600,
                                                    'settings[selectors][]': ['month', 'userAvgSession'],
                    },
                    'avg_session_duration_daily':{
                                                    'settings[filter][date][0][from]': '%s',
                                                    'settings[filter][date][0][to]': '%s',
                                                    'settings[filter][dimension][isVerified][value][]': [255, 1],
                                                    'settings[idCurrency]': 840,
                                                    'settings[tz]': 'Europe/Moscow',
                                                    'settings[precision]': 2,
                                                    'settings[retIndent]': 3600,
                                                    'settings[selectors][]': ['userAvgSession', 'date'],
                    },
                    'active_users_daily':{
                                            'settings[filter][date][0][from]': '%s',
                                            'settings[filter][date][0][to]': '%s',
                                            'settings[filter][dimension][isVerified][value][]': [255, 1],
                                            'settings[idCurrency]': 840,
                                            'settings[tz]': 'Europe/Moscow',
                                            'settings[precision]': 2,
                                            'settings[retIndent]': 3600,
                                            'settings[selectors][]': ['date', 'activeUsers'],
                    },
                    'active_users_monthly':{
                                            'settings[filter][date][0][from]': '%s',
                                            'settings[filter][date][0][to]': '%s',
                                            'settings[filter][dimension][isVerified][value][]': [255, 1],
                                            'settings[idCurrency]': 840,
                                            'settings[tz]': 'Europe/Moscow',
                                            'settings[precision]': 2,
                                            'settings[retIndent]': 3600,
                                            'settings[selectors][]': ['activeUsers', 'month'],
                    },
                    'viewing_depth_daily':{
                                            'settings[filter][date][0][from]': '%s',
                                            'settings[filter][date][0][to]': '%s',
                                            'settings[filter][dimension][isVerified][value][]': [255, 1],
                                            'settings[idCurrency]': 840,
                                            'settings[tz]': 'Europe/Moscow',
                                            'settings[precision]': 2,
                                            'settings[retIndent]': 3600,
                                            'settings[selectors][]': ['uSessionPageDepth', 'date'],
                    },
                    'viewing_depth_monthly':{
                                            'settings[filter][date][0][from]': '%s',
                                            'settings[filter][date][0][to]': '%s',
                                            'settings[filter][dimension][isVerified][value][]': [255, 1],
                                            'settings[idCurrency]': 840,
                                            'settings[tz]': 'Europe/Moscow',
                                            'settings[precision]': 2,
                                            'settings[retIndent]': 3600,
                                            'settings[selectors][]': ['uSessionPageDepth', 'month'],
                    },
                    'banner_clicks_daily':{
                                                'settings[filter][date][0][from]': '%s',
                                                'settings[filter][date][0][to]': '%s',
                                                'settings[filter][dimension][customEventName][value][]': ['news-banner-click'],
                                                'settings[filter][dimension][isVerified][value][]': [255, 1],
                                                'settings[idCurrency]': 840,
                                                'settings[tz]': 'Europe/Moscow',
                                                'settings[precision]': 2,
                                                'settings[retIndent]': 3600,
                                                'settings[selectors][]': ['userUniqEvent', 'customEventParamName', 'customEventParamValue', 'date'],
                    },
                    'banner_clicks_monthly':{
                                                'settings[filter][date][0][from]': '%s',
                                                'settings[filter][date][0][to]': '%s',
                                                'settings[filter][dimension][customEventName][value][]': ['news-banner-click'],
                                                'settings[filter][dimension][isVerified][value][]': [255, 1],
                                                'settings[idCurrency]': 840,
                                                'settings[tz]': 'Europe/Moscow',
                                                'settings[precision]': 2,
                                                'settings[retIndent]': 3600,
                                                'settings[selectors][]': ['userUniqEvent', 'customEventParamName', 'customEventParamValue', 'month'],
                    },
                    'avg_unique_session_duration_monthly':{
                                                            'settings[filter][date][0][from]': '%s',
                                                            'settings[filter][date][0][to]': '%s',
                                                            'settings[filter][dimension][isVerified][value][]': [255, 1],
                                                            'settings[idCurrency]': 840,
                                                            'settings[tz]': 'Europe/Moscow',
                                                            'settings[precision]': 2,
                                                            'settings[retIndent]': 3600,
                                                            'settings[selectors][]': ['month', 'userAvgUniqSession'],
                    },
                    'avg_unique_session_duration_daily':{
                                                            'settings[filter][date][0][from]': '%s',
                                                            'settings[filter][date][0][to]': '%s',
                                                            'settings[filter][dimension][isVerified][value][]': [255, 1],
                                                            'settings[idCurrency]': 840,
                                                            'settings[tz]': 'Europe/Moscow',
                                                            'settings[precision]': 2,
                                                            'settings[retIndent]': 3600,
                                                            'settings[selectors][]': ['userAvgUniqSession', 'date'],
                    },
                    'session_daily':{
                                        'settings[filter][date][0][from]': '%s',
                                        'settings[filter][date][0][to]': '%s',
                                        'settings[idCurrency]': 840,
                                        'settings[tz]': 'Europe/Moscow',
                                        'settings[precision]': 2,
                                        'settings[retIndent]': 3600,
                                        'settings[selectors][]': ['date', 'userCountSession'],
                    },
                    'session_monthly':{
                                        'settings[filter][date][0][from]': '%s',
                                        'settings[filter][date][0][to]': '%s',
                                        'settings[idCurrency]': 840,
                                        'settings[tz]': 'Europe/Moscow',
                                        'settings[precision]': 2,
                                        'settings[retIndent]': 3600,
                                        'settings[selectors][]': ['userCountSession', 'month'],
                    },
                    'pages_activity_daily'  :{
                                        'settings[filter][date][0][from]': '%s',
                                        'settings[filter][date][0][to]': '%s',
                                        'settings[filter][dimension][idApp][value][]': [51144, 51143, 51106, 51107, 51692],
                                        'settings[filter][dimension][idPlatform][value][]': [4],
                                        'settings[filter][dimension][idProject][value][]': [12714],
                                        'settings[idCurrency]': 840,
                                        'settings[tz]': 'Europe/Moscow',
                                        'settings[precision]': 2,
                                        'settings[retIndent]': 3600,
                                        'settings[title]': 'Активность по страницам',
                                        'settings[selectors][]': ['pageUrlFullAddress', 'userCountLaunch', 'userCountHit', 'date'],
                    },
                    'pages_activity_monthly':{
                                            'settings[filter][date][0][from]': '%s',
                                            'settings[filter][date][0][to]': '%s',
                                            'settings[filter][dimension][idApp][value][]': [51144, 51143, 51106, 51107, 51692],
                                            'settings[filter][dimension][idPlatform][value][]': [4],
                                            'settings[filter][dimension][idProject][value][]': [12714],
                                            'settings[idCurrency]': 840,
                                            'settings[tz]': 'Europe/Moscow',
                                            'settings[precision]': 2,
                                            'settings[retIndent]': 3600,
                                            'settings[title]': 'Активность по страницам',
                                            'settings[selectors][]': ['pageUrlFullAddress', 'userCountLaunch', 'userCountHit', 'month']
                    },
                    'mau_daily':{
                                'settings[filter][date][0][from]': '%s',
                                'settings[filter][date][0][to]': '%s',
                                'settings[idCurrency]': 840,
                                'settings[tz]': 'Europe/Moscow',
                                'settings[precision]': 2,
                                'settings[retIndent]': 3600,
                                'settings[selectors][]': ['userMau', 'date'],
                    },
                    'launches_daily':{    
                                        'settings[filter][date][0][from]': '%s',
                                        'settings[filter][date][0][to]': '%s',
                                        'settings[idCurrency]': 840,
                                        'settings[tz]': 'Europe/Moscow',
                                        'settings[precision]': 2,
                                        'settings[retIndent]': 3600,
                                        'settings[selectors][]': ['date', 'userCountLaunch']
                    },
                    'launches_monthly':{
                                        'settings[filter][date][0][from]': '%s',
                                        'settings[filter][date][0][to]': '%s',
                                        'settings[idCurrency]': 840,
                                        'settings[tz]': 'Europe/Moscow',
                                        'settings[precision]': 2,
                                        'settings[retIndent]': 3600,
                                        'settings[selectors][]': ['userCountLaunch', 'month'],
                    },
    },
    'raw_data':{
                'dim_banner':{
                            'event': 'userCustomEvents',
                            'dateFrom': '%s', # Согласно вводным от Никиты Муратова
                            'dateTo': '%s', #  Согласно вводным от Никиты Муратова
                            'timezone': 'Europe/Moscow',
                            'eventName[]': ['news-banner-click'],
                            'params.name[]': ['bannerId', 'title',],
                            'selectors[]': ['params.value']
                },
                'user_custom_events' :{ #Активность, реакции пользователей 
                                    'dateFrom': '%s',
                                    'dateTo': '%s',
                                    'timezone': 'Europe/Moscow',
                                    'idAccount': 13081,
                                    'idProject': '12714',
                                    'event': 'userCustomEvents',
                                    'selectors[]': ['customUserId', 'tsEvent', 'idAppTitle', 'eventName', 'params.name', 'params.value',],
                                    'eventName[]': [
                                        'blog-new',
                                        'blog-new-post',
                                        'blog-view-entry',
                                        'blog-post-click',
                                        'timeline-like',
                                        'timeline-dislike',
                                        'timeline-like_like',
                                        'timeline-like_super',
                                        'timeline-like_sad',
                                        'timeline-like_unacceptable',
                                        'timeline-like_wow',
                                        'timeline-like_laugh',
                                        'timeline-dislike_like',
                                        'timeline-dislike_super',
                                        'timeline-dislike_sad',
                                        'timeline-dislike_unacceptable',
                                        'timeline-dislike_wow',
                                        'timeline-dislike_laugh'
                ],
                },
                'user_sessions':{
                                'event': 'userSessions',
                                'dateFrom': '%s',
                                'dateTo': '%s',
                                'timezone': 'Europe/Moscow',
                                'idAccount': 13081,
                                'idProject': '12714',
                                'selectors[]': ['customUserId', 'tsEvent', 'duration', 'causeAttr', 'causeAttrTitle'],
                }
    }
}

CREDS = {
        'api_user_id':'%s',
        'api_secret_key':'%s'
        }

RENAME_COLUMS = {
                'dau_daily': {
                    'DAU (u)':'dau_u',
                    'Date':'date'
                },
                'dau_monthly':{
                    'DAU (u)':'dau_u',
                    'Month':'month'
                },
                'mau': {
                    'MAU (u)':'mau_u',
                    'Month':'month'
                },
                'avg_session_duration_monthly':{
                    'Month':'month',
                    'Avg. session dur., sec. (u)':'avg_session_duration_sec_u'
                },
                'avg_session_duration_daily':{
                    'Avg. session dur., sec. (u)':'avg_session_duration_sec_u',
                    'Date':'date'
                },
                'active_users_daily':{
                    'Date':'date',
                    'Active users (u)':'active_users_u'
                },
                'active_users_monthly':{
                    'Active users (u)': 'active_users_u',
                    'Month':'month',
                },
                'viewing_depth_daily':{
                    'Page depth (u)':'page_depth_u',
                    'Date':'date'
                },
                'viewing_depth_monthly':{
                    'Page depth (u)': 'page_depth_u',
                    'Month':'month'
                },
                'banner_clicks_daily':{
                    'Parameter value':'banner_id',
                    'Parameter name':'banner_param',                    
                    'Date':'date',
                    'Unique events (u)': 'click_number'
                },
                'banner_clicks_monthly':{
                    'Parameter value':'banner_id',
                    'Parameter name':'banner_param',
                    'Month':'month',
                    'Unique events (u)': 'click_number'            
                },
                'avg_unique_session_duration_monthly':{
                    'Month':'month',
                    'Avg. unique session dur., sec. (u)':'avg_uniq_session_duration_sec_u',
                },
                'avg_unique_session_duration_daily':{
                    'Avg. unique session dur., sec. (u)':'avg_uniq_session_duration_sec_u',
                    'Date':'date'
                },
                'session_daily':{
                    'Date':'date',
                    'Sessions (u)':'sessions_u',                
                },
                'session_monthly':{
                    'Sessions (u)':'sessions_u',
                    'Month':'month'
                },
                'pages_activity_daily':{
                    'Full page URL':'full_page_url',
                    'Launches (u)':'launches_u',
                    'Pageviews (u)':'pageviews_u',
                    'Date':'date'
                },
                'pages_activity_monthly':{
                    'Full page URL':'full_page_url',
                    'Launches (u)':'launches_u',
                    'Pageviews (u)':'pageviews_u',
                    'Month':'month'
                },
                'mau_daily':{
                    'MAU (u)':'mau_u',
                    'Date':'date'
                },
                'launches_daily':{
                    'Date':'date',
                    'Launches (u)':'launches_u',
                },
                'launches_monthly':{
                    'Month':'month',
                    'Launches (u)':'launches_u',
                },
                'user_custom_events':{
                    'customUserId':'custom_user_id',
                    'tsEvent':'event_ts',
                    'idAppTitle':'app_name',
                    'eventName':'event_name',
                    'params.name':'param_name',
                    'params.value':'param_value'
                },
                'user_sessions':{
                    'customUserId': 'custom_user_id',
                    'tsEvent':'event_ts',
                    'duration':'session_duration',
                    'causeAttr':'cause_id',
                    'causeAttrTitle':'cause_name',
                },
}


DISTILLED_CONFIG={
                'dau_daily':{
                            'key_columns':['date'],
                            'sort_columns':['updated_dttm']
                },
                'dau_monthly':{
                                'key_columns':['month'],
                                'sort_columns':['updated_dttm']
                },
                'mau':{
                    'key_columns':['month'],
                    'sort_columns':['updated_dttm']
                },
                'active_users_monthly':{
                    'key_columns':['month'],
                    'sort_columns':['updated_dttm']
                },
                'active_users_daily':{
                    'key_columns':['date'],
                    'sort_columns':['updated_dttm']
                },
                'avg_session_duration_daily':{
                    'key_columns':['date'],
                    'sort_columns':['updated_dttm']
                },
                'avg_session_duration_monthly':{
                    'key_columns':['month'],
                    'sort_columns':['updated_dttm']
                },
                'avg_unique_session_duration_monthly':{
                    'key_columns':['month'],
                    'sort_columns':['updated_dttm']
                },
                'avg_unique_session_duration_daily':{
                    'key_columns':['date'],
                    'sort_columns':['updated_dttm']
                },
                'banner_clicks_monthly':{
                    'key_columns':['banner_id', 'banner_param','month'],
                    'sort_columns':['updated_dttm']
                },
                'banner_clicks_daily':{
                    'key_columns':['banner_id','banner_param','date'],
                    'sort_columns':['updated_dttm']
                },
                'session_monthly':{
                    'key_columns':['month'],
                    'sort_columns':['updated_dttm']
                },
                'session_daily':{
                    'key_columns':['date'],
                    'sort_columns':['updated_dttm']                    
                },
                'viewing_depth_daily':{
                    'key_columns':['date'],
                    'sort_columns':['updated_dttm']                    
                },
                'viewing_depth_monthly':{
                    'key_columns':['month'],
                    'sort_columns':['updated_dttm']                    
                },
                'pages_activity_daily':{
                    'key_columns':['full_page_url','date'],
                    'sort_columns':['updated_dttm']
                },
                'pages_activity_monthly':{
                    'key_columns':['full_page_url','month'],
                    'sort_columns':['updated_dttm']
                },
                'mau_daily':{
                    'key_columns':['date'],
                    'sort_columns':['updated_dttm']   
                },
                'launches_daily':{
                    'key_columns':['date'],
                    'sort_columns':['updated_dttm']       
                },
                'launches_monthly':{
                    'key_columns':['month'],
                    'sort_columns':['updated_dttm']  
                },
                'dim_banner': {
                    'key_columns': ['banner_id'],
                    'sort_columns':['updated_dttm']
                },
                'user_custom_events': {
                    'key_columns': ['custom_user_id', 'event_ts', 'param_name'],
                    'sort_columns':['updated_dttm']
                },
                'user_sessions': {
                    'key_columns':['custom_user_id', 'event_ts'],
                    'sort_columns': ['updated_dttm']
                },
}

STG_CONFIG = {
    'dau_daily':{
                    'columns':[
                        'date DATE',
                        'dau_u INT'
                    ],
                    'columns_to_deduplicate':['date']
    },
    'dau_monthly': {
                    'columns':[
                        'month DATE',
                        'dau_u INT'
                    ],
                    'columns_to_deduplicate':['month'],
    },
    'mau':{
        'columns':[
            'month DATE', 
            'mau_u INT'
        ],
        'columns_to_deduplicate':['month']
    },
    'avg_session_duration_monthly':{
        'columns': [
            'month DATE',
            'avg_session_duration_sec_u INT'
        ],
        'columns_to_deduplicate':['month']
    },
    'avg_session_duration_daily':{
        'columns':[
            'date DATE',
            'avg_session_duration_sec_u INT'
        ],
        'columns_to_deduplicate':['date']
    },
    'active_users_daily':{
        'columns':[
            'date DATE',
            'active_users_u INT'
        ],
        'columns_to_deduplicate':['date']
    },
    'active_users_monthly':{
        'columns':[
            'month DATE',
            'active_users_u INT'
        ],
        'columns_to_deduplicate':['month']
    },
    'viewing_depth_daily':{
        'columns':[
            'date DATE',
            'page_depth_u INT'
        ],
        'columns_to_deduplicate':['date']
    },
    'viewing_depth_monthly':{
        'columns':[
            'month DATE',
            'page_depth_u INT'
        ],
        'columns_to_deduplicate':['month']
    },
    'banner_clicks_daily':{
        'columns':[
            'banner_id TEXT',
            'banner_param TEXT',
            'date DATE',
            'click_number INT'
        ],
        'columns_to_deduplicate':['banner_id', 'banner_param', 'date']
    },
    'banner_clicks_monthly':{
        'columns':[
            'banner_id TEXT',
            'banner_param TEXT',
            'month DATE',
            'click_number INT'
        ],
        'columns_to_deduplicate':['banner_id', 'banner_param','month'],
    },
    'avg_unique_session_duration_monthly':{
        'columns':[
            'month DATE',
            'avg_uniq_session_duration_sec_u INT'
        ],
        'columns_to_deduplicate':['month'],
    },
    'avg_unique_session_duration_daily':{
        'columns':[
            'date DATE',
            'avg_uniq_session_duration_sec_u INT'
        ],
        'columns_to_deduplicate':['date'],
    },
    'session_monthly':{
        'columns':[
            'month DATE',
            'sessions_u INT',
        ],
        'columns_to_deduplicate':['month']
    },
    'session_daily':{
        'columns':[
            'date DATE',
            'sessions_u INT'
        ],
    'columns_to_deduplicate':['date']
    },
    'pages_activity_daily':{
        'columns':[
            'full_page_url TEXT',
            'date DATE',
            'launches_u INT',
            'pageviews_u INT'
        ],
        'columns_to_deduplicate':['full_page_url','date']
    },
    'pages_activity_monthly':{
        'columns':[
            'full_page_url TEXT',
            'month DATE',
            'launches_u INT',
            'pageviews_u INT'
        ],
        'columns_to_deduplicate':['full_page_url','month']
    },
    'mau_daily':{
        'columns':[
            'date DATE',
            'mau_u INT'
        ],
        'columns_to_deduplicate':['date']
    },
    'launches_daily':{
        'columns':[
            'date DATE',
            'launches_u INT'
        ],
    'columns_to_deduplicate':['date']
    },
    'launches_monthly':{
        'columns':[
            'month DATE',
            'launches_u INT'
        ],
        'columns_to_deduplicate':['month']
    },
    'dim_banner': {
        'columns': [
            'banner_id INT',
            'banner_name TEXT'
        ],
        'columns_to_deduplicate':['banner_id']
    },
    'user_custom_events': {
        'columns':[
            'custom_user_id INT',
            'event_ts BIGINT',
            'app_name TEXT',
            'event_name TEXT',
            'param_name TEXT',
            'param_value TEXT', 
        ],
        'columns_to_deduplicate':['custom_user_id', 'event_ts', 'param_name']
    },
    'user_sessions': {
        'columns':[
            'custom_user_id INT',
            'event_ts BIGINT',
            'session_duration INT',
            'cause_id INT',
            'cause_name TEXT'
        ],
        'columns_to_deduplicate':['custom_user_id', 'event_ts']
    }
}

ODS_TABLES = {
                 'dau_daily':{
                                'columns':[
                                            'row_id TEXT',
                                            'date DATE',
                                            'dau_u INT',
                                            'updated_dttm TIMESTAMP'
                                        ],
                                'unique_constraints':['CONSTRAINT dau_daily_pk PRIMARY KEY (date)'],
                                'columns_to_upsert':['date', 'dau_u']
                },
                'dau_monthly':{
                                'columns':[
                                            'row_id TEXT',
                                            'month DATE',
                                            'dau_u INT',
                                            'updated_dttm TIMESTAMP'
                                ],
                                'unique_constraints':['CONSTRAINT dau_monthly_pk PRIMARY KEY (month)'],
                                'columns_to_upsert':['month', 'dau_u']
                },
                'mau':{
                        'columns':[
                                    'row_id TEXT',
                                    'month DATE',
                                    'mau_u INT',
                                    'updated_dttm TIMESTAMP'
                        ],
                        'unique_constraints':['CONSTRAINT mau_pk PRIMARY KEY (month)'],
                        'columns_to_upsert':['month', 'mau_u']
                },
                'avg_session_duration_monthly':{
                                                'columns':[
                                                            'row_id TEXT',
                                                            'month DATE',
                                                            'avg_session_duration_sec_u INT',
                                                            'updated_dttm TIMESTAMP'
                                                ],
                                                'unique_constraints':['CONSTRAINT avg_session_duration_monthly_pk PRIMARY KEY (month)'],
                                                'columns_to_upsert':['month', 'avg_session_duration_sec_u']
                },
                'avg_session_duration_daily':{
                                                'columns':[
                                                            'row_id TEXT',
                                                            'date DATE',
                                                            'avg_session_duration_sec_u INT',
                                                            'updated_dttm TIMESTAMP'
                                                ],
                                                'unique_constraints':['CONSTRAINT avg_session_duration_daily_pk PRIMARY KEY (date)'],
                                                'columns_to_upsert':['date', 'avg_session_duration_sec_u']
                },
                'active_users_daily':{
                                    'columns':[
                                                    'row_id TEXT',
                                                    'date DATE',
                                                    'active_users_u INT',
                                                    'updated_dttm TIMESTAMP'
                                    ],
                                    'unique_constraints':['CONSTRAINT active_users_daily_pk PRIMARY KEY (date)'],
                                    'columns_to_upsert':['date', 'active_users_u']    
                },
                'active_users_monthly':{
                                        'columns':[
                                            'row_id TEXT',
                                            'month DATE',
                                            'active_users_u INT',
                                            'updated_dttm TIMESTAMP'
                                        ],
                                        'unique_constraints':['CONSTRAINT active_users_dailyactive_users_monthly_pk PRIMARY KEY (month)'],
                                        'columns_to_upsert':['month', 'active_users_u']    
                },
                'viewing_depth_daily':{
                                        'columns':[
                                            'row_id TEXT',
                                            'date DATE',
                                            'page_depth_u INT',
                                            'updated_dttm TIMESTAMP'
                                        ],
                                        'unique_constraints':['CONSTRAINT viewing_depth_daily_pk PRIMARY KEY (date)'],
                                        'columns_to_upsert':['date', 'page_depth_u']    
                },
                'viewing_depth_monthly':{
                                        'columns':[
                                            'row_id TEXT',
                                            'month DATE',
                                            'page_depth_u INT',
                                            'updated_dttm TIMESTAMP'                                            
                                        ],
                                        'unique_constraints':['CONSTRAINT viewing_depth_monthly_pk PRIMARY KEY (month)'],
                                        'columns_to_upsert':['month', 'page_depth_u']                                            
                },
                'banner_clicks_daily':{
                                        'columns':[
                                            'row_id TEXT',
                                            'banner_id TEXT',
                                            'banner_param TEXT',
                                            'date DATE',
                                            'click_number INT',
                                            'updated_dttm TIMESTAMP'
                                        ],
                                        'unique_constraints':['CONSTRAINT banner_clicks_daily_pk PRIMARY KEY (banner_id, banner_param, date)'],
                                        'columns_to_upsert':['banner_id', 'banner_param', 'date', 'click_number']                                          
                },
                'banner_clicks_monthly':{
                                        'columns':[
                                            'row_id TEXT',
                                            'banner_id TEXT',
                                            'banner_param TEXT',
                                            'month DATE',
                                            'click_number INT',
                                            'updated_dttm TIMESTAMP'
                                        ],
                                        'unique_constraints':['CONSTRAINT banner_clicks_monthly_pk PRIMARY KEY (banner_id, banner_param, month)'],
                                        'columns_to_upsert':['banner_id', 'banner_param', 'month', 'click_number']                                       
                },
                'avg_unique_session_duration_monthly': {
                                                        'columns':[
                                                            'row_id TEXT',
                                                            'month DATE',
                                                            'avg_uniq_session_duration_sec_u INT',
                                                            'updated_dttm TIMESTAMP'
                                                        ],
                                                        'unique_constraints':['CONSTRAINT avg_unique_session_duration_monthly_pk PRIMARY KEY (month)'],
                                                        'columns_to_upsert':['month', 'avg_uniq_session_duration_sec_u']
                },
                'avg_unique_session_duration_daily':{
                                                    'columns':[
                                                            'row_id TEXT',
                                                            'date DATE',
                                                            'avg_uniq_session_duration_sec_u INT',
                                                            'updated_dttm TIMESTAMP'
                                                    ],
                                                    'unique_constraints':['CONSTRAINT avg_unique_session_duration_daily_pk PRIMARY KEY (date)'],
                                                    'columns_to_upsert':['date', 'avg_uniq_session_duration_sec_u']                                                    
                },
                'session_monthly':{
                                    'columns':[
                                        'row_id TEXT',
                                        'month DATE',
                                        'sessions_u INT',
                                        'updated_dttm TIMESTAMP'
                                    ],
                                    'unique_constraints':['CONSTRAINT session_monthly_pk PRIMARY KEY (month)'],
                                    'columns_to_upsert':['month', 'sessions_u']
                },
                'session_daily':{
                                'columns':[
                                    'row_id TEXT',
                                    'date DATE',
                                    'sessions_u INT',
                                    'updated_dttm TIMESTAMP'                                    
                                ],
                                'unique_constraints':['CONSTRAINT session_daily_pk PRIMARY KEY (date)'],
                                'columns_to_upsert':['date', 'sessions_u']                                
                },
                'pages_activity_daily':{
                    'columns':[
                        'row_id TEXT',
                        'full_page_url TEXT',
                        'date DATE',
                        'launches_u INT',
                        'pageviews_u INT',
                        'updated_dttm TIMESTAMP'
                    ],
                    'unique_constraints':['CONSTRAINT pages_activity_daily_pk PRIMARY KEY (full_page_url, date)'],
                    'columns_to_upsert':['full_page_url', 'date', 'launches_u', 'pageviews_u']                                
                },
                'pages_activity_monthly':{
                    'columns':[
                        'row_id TEXT',
                        'month DATE',
                        'full_page_url TEXT',
                        'launches_u INT',
                        'pageviews_u INT',
                        'updated_dttm TIMESTAMP'
                    ],
                    'unique_constraints':['CONSTRAINT pages_activity_monthly_pk PRIMARY KEY (full_page_url, month)'],
                    'columns_to_upsert':['full_page_url', 'month', 'launches_u', 'pageviews_u']                                                    
                },
                'mau_daily':{
                    'columns':[
                        'row_id TEXT',
                        'date DATE',
                        'mau_u INT',
                        'updated_dttm TIMESTAMP'
                    ],
                    'unique_constraints':['CONSTRAINT mau_daily_pk PRIMARY KEY (date)'],
                    'columns_to_upsert':['date', 'mau_u']                                                    
                },
                'launches_daily':{
                    'columns':[
                        'row_id TEXT',
                        'date DATE',
                        'launches_u INT',
                        'updated_dttm TIMESTAMP'
                    ],
                    'unique_constraints':['CONSTRAINT launches_daily_pk PRIMARY KEY (date)'],
                    'columns_to_upsert':['date', 'launches_u']                    
                },
                'launches_monthly':{
                    'columns':[
                        'row_id TEXT',
                        'month DATE',
                        'launches_u INT',
                        'updated_dttm TIMESTAMP'
                    ],
                    'unique_constraints':['CONSTRAINT launches_monthly_pk PRIMARY KEY (month)'],
                    'columns_to_upsert':['month', 'launches_u']                    
                },
                'dim_banner': {
                        'columns': [
                            'banner_id INT',
                            'banner_name TEXT',
                            'updated_dttm TIMESTAMP'
                        ],
                        'unique_constraints':['CONSTRAINT dim_banner_pk PRIMARY KEY (banner_id)'],
                        'columns_to_upsert':['banner_id', 'banner_name']
                },
                'user_custom_events': {
                                'columns':[
                                    'row_id TEXT',
                                    'custom_user_id INT',
                                    'event_ts BIGINT',
                                    'app_name TEXT',
                                    'event_name TEXT',
                                    'param_name TEXT',
                                    'param_value TEXT',
                                    'updated_dttm TIMESTAMP'
                                ],
                                'unique_constraints':['CONSTRAINT user_custom_events_pk PRIMARY KEY (custom_user_id, event_ts, param_name)'],
                                'columns_to_upsert':[
                                                'custom_user_id',
                                                'event_ts',
                                                'app_name',
                                                'event_name',
                                                'param_name',
                                                'param_value'
                                ]
                },
                'user_sessions': {
                                'columns':[
                                    'row_id TEXT',
                                    'custom_user_id INT',
                                    'event_ts BIGINT',
                                    'session_duration INT',
                                    'cause_id INT',
                                    'cause_name TEXT',
                                    'updated_dttm TIMESTAMP'
                                ],
                                'unique_constraints':['CONSTRAINT user_sessions_pk PRIMARY KEY (custom_user_id, event_ts)'],
                                'columns_to_upsert':[
                                                'custom_user_id',
                                                'event_ts',
                                                'session_duration',
                                                'cause_id',
                                                'cause_name',
                                ]
                }
}