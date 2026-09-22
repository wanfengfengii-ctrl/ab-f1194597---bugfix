-- Wake long-poll waiters when history is compacted.
--
-- try_advance() (migration 0002) notifies the 'telemetry_events' channel only
-- when the visible watermark advances. Compaction inserts a checkpoint and
-- deletes the covered events WITHOUT touching devices.high_watermark, so a
-- /wait client blocked on that channel would not learn that its afterSequence
-- had fallen behind a checkpoint until its poll timeout expired.
--
-- /wait issues LISTEN before reading (high_watermark, latest checkpoint) and
-- re-reads both after every wakeup, delivering HTTP 410 with the verifiable
-- checkpoint when afterSequence is behind it. Notifying on checkpoint insert
-- makes a compaction that lands while a waiter is blocked take effect at once;
-- NOTIFY is delivered at COMMIT (exactly like the ingest notification), so it
-- can never be lost by the LISTEN -> read ordering.
CREATE OR REPLACE FUNCTION notify_checkpoint_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM pg_notify('telemetry_events', NEW.device_id);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_checkpoints_notify ON checkpoints;
CREATE TRIGGER trg_checkpoints_notify
  AFTER INSERT ON checkpoints
  FOR EACH ROW
  EXECUTE FUNCTION notify_checkpoint_insert();
