SELECT AVG(TIMESTAMP_DIFF(EndTimestampUtc, StartTimestampUtc, MINUTE)) AS AvgDuration
FROM (
  SELECT 
    *,
    Data.TimestampUtc AS EndTimestampUtc,
    LEAD(Data.TimestampUtc) OVER (PARTITION BY Data.AppointmentId ORDER BY Data.TimestampUtc DESC) as StartTimestampUtc
  FROM `appointment-streaming-test.appointment_df_out.events_raw`
) AS pivot
WHERE Type = "AppointmentComplete"