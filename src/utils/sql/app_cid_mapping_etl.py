def sql(query_table, query_date):
    querystring = """
    SELECT
    CURRENT_DATE("Asia/Taipei") AS etl_dt,
    DATETIME(TIMESTAMP_SECONDS(visitStartTime),"Asia/Taipei") AS visitDateTime,
    DATE(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDate,
    fullVisitorId,
    customDimensions.value AS customDimensions_value
    FROM `neat-motif-123006.{}.ga_sessions_*`, 
    UNNEST(customDimensions) as customDimensions
    WHERE _TABLE_SUFFIX IN {}
    AND customDimensions.value IS NOT NULL
    """.format(query_table, query_date)  
    return querystring