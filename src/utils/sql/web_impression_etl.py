def sql(query_table, query_date):
    querystring = """
    SELECT DISTINCT 
    CURRENT_DATE("Asia/Taipei") AS etl_dt,
    DATETIME(TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), 
    INTERVAL hits.time MILLISECOND), "Asia/Taipei") AS eventDateTime,
    DATETIME(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDateTime,
    DATE(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDate,
    fullVisitorId,
    REGEXP_EXTRACT(clientId, r"^[a-zA-Z0-9_.+-]+") AS clientId,
    device.deviceCategory AS deviceCategory,
    hits.eventInfo.eventLabel AS hits_eventInfo_eventLabel
    FROM
    `neat-motif-123006.{}.ga_sessions_*`,
    UNNEST(hits) AS hits
    WHERE
    _TABLE_SUFFIX IN {} 
    AND hits.type = "EVENT"
    AND hits.eventInfo.eventCategory = "官網隨機banner曝光追蹤"
    AND clientId IS NOT NULL 
    """.format(query_table, query_date)
    return querystring