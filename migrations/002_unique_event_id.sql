-- event_id is the provider's stable delivery key. Without a unique
-- constraint, concurrent redeliveries can insert twice and double-count.
DELETE FROM events a USING events b
 WHERE a.id > b.id AND a.event_id = b.event_id;

DROP INDEX IF EXISTS idx_events_event_id;
CREATE UNIQUE INDEX IF NOT EXISTS events_event_id_uidx ON events (event_id);
