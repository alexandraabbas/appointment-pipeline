SELECT 
    AVG(TIMESTAMP_DIFF(EndTimestampUtc, StartTimestampUtc, MINUTE)) AS AvgDuration,
    Discipline
FROM (
  SELECT 
    *,
    Data.TimestampUtc AS EndTimestampUtc,
    LEAD(Data.TimestampUtc) OVER (PARTITION BY Data.AppointmentId, Discipline ORDER BY Data.TimestampUtc DESC) as StartTimestampUtc
  FROM `appointment-streaming-test.appointment_df_out.events_raw`
  CROSS JOIN UNNEST(Data.Discipline) AS Discipline
) AS pivot
WHERE Type = "AppointmentComplete"
GROUP BY Discipline
