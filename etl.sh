#!/bin/bash
python3 etl.py -t web_event >> logs/web_event_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t web_campaign_impression >> logs/web_campaign_impression_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t web_custno_mapping >> logs/web_custno_mapping_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t web_view >> logs/web_view_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_cid_mapping_android >> logs/app_cid_mapping_android_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_cid_mapping_ios >> logs/app_cid_mapping_ios_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_event_android >> logs/app_event_android_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_event_ios >> logs/app_event_ios_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_native_android >> logs/app_native_android_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_native_ios >> logs/app_native_ios_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_custno_mapping_android >> logs/app_custno_mapping_android_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t app_custno_mapping_ios >> logs/app_custno_mapping_ios_$(date +\%Y\%m\%d).log 2>&1
python3 etl.py -t aaid_idfa_custno_mapping >> aaid_idfa_custno_mapping_$(date +\%Y\%m\%d).log 2>&1
