def sql(query_table, query_date):
    querystring = """
    SELECT
      CURRENT_DATE("Asia/Taipei") AS etl_dt,
      event_date,

      DATETIME(TIMESTAMP_MICROS(event_timestamp),"Asia/Taipei") AS event_timestamp,
      user_id,
      device.advertising_id AS advertising_id,
      device.operating_system AS operating_system
    FROM
      `esun-mobile-bank.analytics_{}.events_*`
    WHERE _TABLE_SUFFIX IN {} AND
      (device.advertising_id IS NOT NULL OR device.advertising_id != '') AND
      user_id IS NOT NULL
    """.format(query_table, query_date)
    
    return querystring
