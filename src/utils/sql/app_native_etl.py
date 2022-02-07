def sql(query_table, query_date):
    querystring = """
    SELECT DISTINCT
    CURRENT_DATE("Asia/Taipei") AS etl_dt,
    DATETIME(TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND),
    "Asia/Taipei")AS viewDateTime,
    DATETIME(TIMESTAMP_SECONDS(visitStartTime),
    "Asia/Taipei") AS visitDateTime,
    DATE(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDate,
    fullVisitorId,
    device.mobileDeviceInfo AS mobileDeviceInfo,
    device.mobileDeviceBranding AS mobileDeviceBranding,
    device.mobileDeviceModel AS mobileDeviceModel,
    device.operatingSystem AS operatingSystem,
    device.operatingSystemVersion AS operatingSystemVersion,
    hits.appInfo.appVersion AS hits_appInfo_appVersion,
    hits.appInfo.screenName AS hits_appInfo_screenName,
    hits.appInfo.landingScreenName AS hits_appInfo_landingScreenName,
    hits.appInfo.exitScreenName AS hits_appInfo_exitScreenName,
    hits.appInfo.screenDepth AS hits_appInfo_screenDepth,
    CAST(((LEAD(hits.time, 1) OVER 
    (PARTITION BY fullVisitorId, visitNumber ORDER BY hits.time ASC))-hits.time)*0.001 AS FLOAT64) AS timeOnPage
    FROM
    `neat-motif-123006.{}.ga_sessions_*`,
    UNNEST(hits) AS hits
    WHERE
    _TABLE_SUFFIX IN {}
    AND hits.type = 'APPVIEW'
    """.format(query_table, query_date)
    
    return querystring