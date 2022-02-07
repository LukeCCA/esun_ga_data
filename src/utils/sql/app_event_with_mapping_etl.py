def sql(query_table, query_date):
    querystring = """
    SELECT DISTINCT
    CURRENT_DATE("Asia/Taipei") AS etl_dt,
    DATETIME(TIMESTAMP_SECONDS(visitStartTime),"Asia/Taipei") AS visitDateTime,
    DATETIME(TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime),
    INTERVAL hits.time MILLISECOND),"Asia/Taipei") AS eventDateTime,
    DATE(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDate,
    fullVisitorId,
    hits.eventInfo.eventCategory AS hits_eventInfo_eventCategory,
    hits.eventInfo.eventAction AS hits_eventInfo_eventAction,
    hits.eventInfo.eventLabel AS hits_eventInfo_eventLabel,
    hits.eventInfo.eventValue AS hits_eventInfo_eventValue
    FROM
    `neat-motif-123006.{}.ga_sessions_*`,
    UNNEST(hits) AS hits
    WHERE
    _TABLE_SUFFIX IN {} 
    AND hits.type = 'EVENT'
    """.format(query_table, query_date)
    
    return querystring