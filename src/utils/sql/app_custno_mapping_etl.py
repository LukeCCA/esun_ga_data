def sql(query_table, query_date):
    querystring = """
    SELECT DISTINCT
    CURRENT_DATE("Asia/Taipei") AS etl_dt,
    DATETIME(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDateTime,
    DATE(TIMESTAMP_SECONDS(visitStartTime), "Asia/Taipei") AS visitDate,
    fullVisitorId,
    REGEXP_EXTRACT(clientId, r"^[a-zA-Z0-9_.+-]+") AS clientId,
    customDimensions.value AS customDimensions_value
    FROM `neat-motif-123006.{}.ga_sessions_*`,
    UNNEST(customDimensions) AS customDimensions
    WHERE _TABLE_SUFFIX IN {}
    AND customDimensions.index=3
    AND customDimensions.value!=''
    AND clientId IS NOT NULL
    """.format(query_table, query_date) 
    return querystring