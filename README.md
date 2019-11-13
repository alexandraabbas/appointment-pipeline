# Appointment streaming pipeline

## TODO:

- [ ] Pubsub publisher (Code, VM, topic)
- [ ] PubSub subscription
- [ ] BigQuery table schema file
- [ ] Create BigQuery table
- [ ] AppointmentStreamingPipeline
  - [ ] Read from PubSub (using subscription)
  - [ ] Parse messages
  - [ ] Validate messages (Write invalid messages to GCS?)
  - [ ] Flatten Data field
  - [ ] Convert to TableRow (Discipline is REPEATED and may not exist!)
  - [ ] Write to BigQuery
- [ ] Create BigQuery view (to query current state of appointments)
- [ ] Write README
  - [ ] Architecture diagram
  - [ ] Design and decisions
  - [ ] Considerations
    - How you adapt to changes in schema?
    - How you verify that the end result is correct?
    - What happens if you get a broken event, and how would you handle it?
