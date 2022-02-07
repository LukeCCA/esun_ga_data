#!/bin/bash
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t web_event >> /home/ESB16053/logs/web_event_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t web_campaign_impression >> /home/ESB16053/logs/web_campaign_impression_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t web_custno_mapping >> /home/ESB16053/logs/web_custno_mapping_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t web_view >> /home/ESB16053/logs/web_view_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_cid_mapping_android >> /home/ESB16053/logs/app_cid_mapping_android_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_cid_mapping_ios >> /home/ESB16053/logs/app_cid_mapping_ios_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_event_android >> /home/ESB16053/logs/app_event_android_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_event_ios >> /home/ESB16053/logs/app_event_ios_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_native_android >> /home/ESB16053/logs/app_native_android_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_native_ios >> /home/ESB16053/logs/app_native_ios_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_custno_mapping_android >> /home/ESB16053/logs/app_custno_mapping_android_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t app_custno_mapping_ios >> /home/ESB16053/logs/app_custno_mapping_ios_$(date +\%Y\%m\%d).log 2>&1
/usr/bin/python3 /home/ESB16053/chen/ga_etl/etl.py -t aaid_idfa_custno_mapping >> /home/ESB16053/logs/aaid_idfa_custno_mapping_$(date +\%Y\%m\%d).log 2>&1