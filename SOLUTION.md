# Solution

## What was broken

Four defects, all visible from the ops report:

1. **Duplicate events / drifting call-counts.** Dedup was a non-atomic `SELECT` then `INSERT`. There was no unique constraint on `event_id`, so concurrent redeliveries both saw “new”, both inserted, and both incremented `account_stats`. Sequential retries happened to pass because the first write landed before the next `EventExists` check.

2. **Recordings never marked processed, no logs.** `processRecording` ran in a goroutine with the HTTP request context. That context is cancelled as soon as the handler returns 200, so `MarkRecordingProcessed` failed with `context canceled`. The error hit `// TODO: handle` and was dropped.

3. **In-flight work vanished on deploy.** Recording work lived only in those goroutines. A restart killed them; nothing durable remembered that `recording_processed` was still false.

4. **Stats endpoint emptied on restart.** `GET /accounts/{id}/stats` read an in-memory map that started empty and was never loaded from `account_stats`. `Cache.Record` also mutated the map without the mutex that `Get` used, so concurrent ingest raced.

## Deduplication

Postgres unique index on `event_id`, plus one transaction that inserts the event (`ON CONFLICT DO NOTHING`), upserts the call, and increments stats. A conflict means “already ingested” — we commit nothing extra and return 200.

I did not use Redis `SETNX`. Redis is not the system of record for events or counts; a crash between `SETNX` and the SQL writes (or a flush) either double-counts or drops a delivery. The unique constraint lives next to the rows it protects, and the transaction keeps insert and increment from diverging.

Redis is used as a *wakeup queue* for recording jobs. The durable truth for those jobs is still `calls.recording_processed`. On start we re-enqueue anything still false, so a deploy cannot strand work even though Compose Redis has no disk.

## At 10,000 webhooks/sec

Keep the unique insert as the idempotency gate, but stop doing per-request stats increments in the same hot transaction: batch or use a counter table keyed by a short time bucket. Run recording workers from Redis Streams with consumer groups (ack + retry) instead of a LIST, and shard or partition `events` when the `event_id` index becomes the write bottleneck. The in-memory stats cache would have to move to Redis (or be dropped in favour of the table) the moment there is more than one ingest process.
