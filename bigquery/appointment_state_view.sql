-- Finds latest rows for each AppointmentId, used for state_view BigQuery view
SELECT * except (rank)
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY Data.AppointmentId ORDER BY Data.TimestampUtc DESC) as rank
  FROM `appointment-streaming-test.appointment_df_out.events_raw`) AS latest
WHERE rank = 1;
