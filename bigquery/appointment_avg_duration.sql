WITH base AS (
  SELECT "AppointmentBooked" AS Type, "3cb0f939-9398-4d29-a28f-2a1a3a6ce3b2" AS AppointmentId, CAST("2017-05-14T19:12:32Z" AS TIMESTAMP) AS TimestampUtc UNION ALL
  SELECT "AppointmentComplete" AS Type, "3cb0f939-9398-4d29-a28f-2a1a3a6ce3b2" AS AppointmentId, CAST("2017-05-14T19:18:32Z" AS TIMESTAMP) AS TimestampUtc UNION ALL
  SELECT "AppointmentBooked" AS Type, "a7d86ca5-7541-4c86-a7ad-1bec2b070b3c" AS AppointmentId, CAST("2017-05-14T19:20:33Z" AS TIMESTAMP) AS TimestampUtc UNION ALL
  SELECT "AppointmentComplete" AS Type, "a7d86ca5-7541-4c86-a7ad-1bec2b070b3c" AS AppointmentId, CAST("2017-05-14T19:22:33Z"AS TIMESTAMP) AS TimestampUtc
)
SELECT AVG(TIMESTAMP_DIFF(EndTimestampUtc, StartTimestampUtc, MINUTE)) AS AvgDuration
FROM (
  SELECT 
    *,
    TimestampUtc AS EndTimestampUtc,
    LEAD(TimestampUtc) OVER (PARTITION BY AppointmentId ORDER BY TimestampUtc DESC) as StartTimestampUtc
  FROM base
) AS pivot
WHERE Type = "AppointmentComplete"