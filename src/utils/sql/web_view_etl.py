def sql(query_table, query_date):
    querystring = """
    SELECT DISTINCT
    CURRENT_DATE("Asia/Taipei") AS etl_dt,
    DATETIME(TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL hits.time MILLISECOND),
    "Asia/Taipei") AS viewDateTime,
    DATETIME(TIMESTAMP_SECONDS(visitStartTime),
    "Asia/Taipei") AS visitDateTime,
    DATE(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDate,
    fullVisitorId,
    REGEXP_EXTRACT(clientId, r"^[a-zA-Z0-9_.+-]+") AS clientId,
    hits.page.pagePath AS pagePath,
    hits.page.pageTitle AS pageTitle,
    device.deviceCategory AS deviceCategory,
    ((LEAD(hits.time, 1) OVER (PARTITION BY fullVisitorId, 
    visitNumber ORDER BY hits.time ASC) - hits.time)*0.001) AS timeOnPage,
    trafficSource.adContent AS trafficSource_adContent,
    trafficSource.campaign AS trafficSource_campaign,
    trafficSource.keyword AS trafficSource_keyword,
    trafficSource.medium AS trafficSource_medium,
    trafficSource.source AS trafficSource_source
    FROM `neat-motif-123006.{}.ga_sessions_*`,
    UNNEST(hits) AS hits
    WHERE _TABLE_SUFFIX IN {}
    AND hits.type = 'PAGE'
    AND hits.page.pagePath IS NOT NULL
    AND clientId IS NOT NULL
    """.format(query_table, query_date)
    return querystring