-- Wake long-poll /wait clients when history is compacted.
--
-- A /wait caller can be parked long before any checkpoint exists. Compaction
-- inserts a checkpoint and deletes the covered events in one transaction
-- (src/services/compaction.mjs); a waiter whose afterSequence falls behind that
-- checkpoint must receive HTTP 410 with the verifiable recovery point, exactly
-- like a paginated /events read - never a 200 that silently skips the deleted
-- prefix.
--
-- The ingest path already notifies the 'telemetry_events' channel
-- (0002_advance.sql). Compaction uses a SEPARATE channel: additive advancement
-- and "the prefix may have been deleted behind you" are different wakeup
-- reasons, and a waiter that has caught up to the watermark can ignore a
-- compaction notification without an extra checkpoint read.
--
-- LISTEN/NOTIFY ordering mirrors the ingest guarantee: waiters issue LISTEN
-- before taking their state snapshot, and NOTIFY is only delivered at COMMIT,
-- so a checkpoint committed between the two is still observed and a rolled
-- back compaction never wakes anyone.
CREATE OR REPLACE FUNCTION notify_checkpoint_inserted() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM pg_notify('telemetry_compacted', NEW.device_id);
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_checkpoint_notify
  AFTER INSERT ON checkpoints
  FOR EACH ROW EXECUTE FUNCTION notify_checkpoint_inserted();
