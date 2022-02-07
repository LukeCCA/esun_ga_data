from .sql.aaid_idfa_custno_mapping import sql as aaid_idfa_custno_mapping
from .sql.app_cid_mapping_etl import sql as app_cid_mapping_etl
from .sql.app_custno_mapping_etl import sql as app_custno_mapping_etl
from .sql.app_event_etl import sql as app_event_etl
from .sql.app_event_with_mapping_etl import sql as app_event_with_mapping_etl
from .sql.app_native_etl import sql as app_native_etl
from .sql.web_custno_mapping_etl import sql as web_custno_mapping_etl
from .sql.web_event_etl import sql as web_event_etl
from .sql.web_impression_etl import sql as web_impression_etl
from .sql.web_view_etl import sql as web_view_etl

configs = {'web_event': {
    'generate_table_name': 'web_event',
    'query_table_name': '125211785',
    'table_columns': ['etl_dt', 'eventDateTime', 'visitDateTime', 'visitDate', 'fullVisitorId',
                      'clientId', 'deviceCategory', 'deviceBrowser', 'hits_eventInfo_eventCategory',
                      'hits_eventInfo_eventAction', 'hits_eventInfo_eventLabel', 'hits_eventInfo_eventValue'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': web_event_etl},
      
           'web_campaign_impression': {
    'generate_table_name': 'web_campaign_impression',
    'query_table_name': '125211785',
    'table_columns': ['etl_dt', 'eventDateTime', 'visitDateTime', 'visitDate', 
                      'fullVisitorId', 'deviceCategory', 'hits_eventInfo_eventLabel', 'clientId'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': web_impression_etl},
          
           'web_custno_mapping': {
    'generate_table_name': 'web_custno_mapping',
    'query_table_name': '125211785',
    'table_columns': ['etl_dt', 'visitDateTime', 'visitDate', 'fullVisitorId', 'clientId',
                      'customDimensions_value'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_custno_mapping_etl},
                        
           'web_view': {
    'generate_table_name': 'web_view',
    'query_table_name': '125211785',
    'table_columns': ['etl_dt', 'viewDateTime', 'visitDateTime', 'visitDate', 'fullVisitorId',
                      'clientId', 'pagePath', 'pageTitle', 'deviceCategory', 'timeOnPage',
                      'trafficSource_adContent', 'trafficSource_campaign', 'trafficSource_keyword',
                      'trafficSource_medium', 'trafficSource_source'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': web_view_etl},
                        
           'app_cid_mapping_android': {
    'generate_table_name': 'app_cid_mapping_android',
    'query_table_name': '118454301',
    'table_columns': ['etl_dt','visitDateTime','visitDate','fullVisitorId','customDimensions_value'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_cid_mapping_etl},
                        
           'app_cid_mapping_ios': {
    'generate_table_name': 'app_cid_mapping_ios',
    'query_table_name': '138367231',
    'table_columns': ['etl_dt','visitDateTime','visitDate','fullVisitorId','customDimensions_value'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_cid_mapping_etl},

           'app_event_android': {
    'generate_table_name': 'app_event_android',
    'query_table_name': '118454301',
    'table_columns': ['etl_dt','visitDateTime','eventDateTime',
                      'visitDate','fullVisitorId','hits_eventInfo_eventCategory',
                      'hits_eventInfo_eventAction','hits_eventInfo_eventLabel','hits_eventInfo_eventValue'],
    'date_col': 'visitDate',
    'cred':'/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_event_etl},

           'app_event_ios': {
    'generate_table_name': 'app_event_ios',
    'query_table_name': '138367231',
    'table_columns': ['etl_dt','visitDateTime','eventDateTime',
                      'visitDate','fullVisitorId','hits_eventInfo_eventCategory',
                      'hits_eventInfo_eventAction','hits_eventInfo_eventLabel','hits_eventInfo_eventValue'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_event_etl},

           'app_native_android': {
    'generate_table_name': 'app_native_android',
    'query_table_name': '118454301',
    'table_columns': ['etl_dt', 'viewDateTime', 'visitDateTime', 'visitDate', 'fullVisitorId',
                      'timeOnPage', 'mobileDeviceInfo', 'mobileDeviceBranding', 'operatingSystem',
                      'operatingSystemVersion', 'mobileDeviceModel', 'hits_appInfo_appVersion',
                      'hits_appInfo_screenName', 'hits_appInfo_landingScreenName',
                      'hits_appInfo_exitScreenName', 'hits_appInfo_screenDepth'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_native_etl},

           'app_native_ios': {
    'generate_table_name': 'app_native_ios',
    'query_table_name': '138367231',
    'table_columns': ['etl_dt', 'viewDateTime', 'visitDateTime', 'visitDate', 'fullVisitorId',
                      'timeOnPage', 'mobileDeviceInfo', 'mobileDeviceBranding', 'operatingSystem',
                      'operatingSystemVersion', 'mobileDeviceModel', 'hits_appInfo_appVersion',
                      'hits_appInfo_screenName', 'hits_appInfo_landingScreenName',
                      'hits_appInfo_exitScreenName', 'hits_appInfo_screenDepth'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_native_etl},

           'app_custno_mapping_android': {
    'generate_table_name': 'app_custno_mapping_android',
    'query_table_name': '118454301',
    'table_columns': ['etl_dt', 'visitDateTime', 'visitDate', 'fullVisitorId', 'clientId',
                      'customDimensions_value'],
    'date_col': 'visitDate',
    'cred':'/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_custno_mapping_etl},

           'app_custno_mapping_ios': {
    'generate_table_name': 'app_custno_mapping_ios',
    'query_table_name': '138367231',
    'table_columns': ['etl_dt', 'visitDateTime', 'visitDate', 'fullVisitorId', 'clientId',
                      'customDimensions_value'],
    'date_col': 'visitDate',
    'cred': '/mnt/esun-crawler-code/ga_etl/.ga_key/GA360-dfc33d0beb96.json',
    'sql_generater': app_custno_mapping_etl},

           'aaid_idfa_custno_mapping': {
    'generate_table_name': 'aaid_idfa_custno_mapping',
    'query_table_name': '213960404',
    'table_columns': ['etl_dt', 'event_date', 'event_timestamp', 'user_id', 'advertising_id',
                      'operating_system'],
    'date_col': 'event_date',
    'cred':'/mnt/esun-crawler-code/ga_etl/.ga_key/esun-mobile-bank-66941d091162.json',
    'sql_generater': aaid_idfa_custno_mapping}}
