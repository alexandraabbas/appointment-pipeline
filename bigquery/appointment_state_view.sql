WITH base AS (
  SELECT "AppointmentBooked" AS Type, "3cb0f939-9398-4d29-a28f-2a1a3a6ce3b2" AS AppointmentId, CAST("2017-05-14T19:12:32Z" AS TIMESTAMP) AS TimestampUtc UNION ALL
  SELECT "AppointmentComplete" AS Type, "3cb0f939-9398-4d29-a28f-2a1a3a6ce3b2" AS AppointmentId, CAST("2017-05-14T19:18:32Z" AS TIMESTAMP) AS TimestampUtc
)
SELECT * except (rank)
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY AppointmentId ORDER BY TimestampUtc DESC) as rank
  FROM base) AS latest
WHERE rank = 1;
