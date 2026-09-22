'use strict';
// Consecutive-event reads.
//
// Fixed views: the first page request snapshots (device, highWatermark) into
// read_views and returns viewHighWatermark plus an unforgeable cursor. Every
// later page with that cursor reads ONLY data inside the view; events that
// commit afterwards can never mix in. The cursor is signed and binds (view,
// device, position): tampering, cross-device use and parameter contradictions
// are all rejected.
//
// Waiting: /wait uses LISTEN/NOTIFY on two channels (telemetry_events for
// commits, telemetry_compacted for checkpoint inserts). The order is
// LISTEN -> snapshot -> block, so a notification produced between the snapshot
// and the wait cannot be lost. Every state snapshot (latest checkpoint,
// watermark, event batch) is taken in one transaction holding a FOR SHARE lock
// on the device row: compaction and ingest both take FOR UPDATE, so the
// checkpoint-and-delete can never be observed half-applied. A waiter whose
// afterSequence is behind a checkpoint gets the same 410 recovery response as a
// paginated read - never a 200 that silently skips the compacted prefix. On
// timeout an empty page is returned. Client disconnect releases the dedicated
// connection back to the pool immediately.

import { randomUUID } from 'node:crypto';
import { withTransaction, withClientTransaction } from '../db/pool.mjs';
import { errors } from '../errors.mjs';
import { signCursor } from '../crypto/checkpoint.mjs';
import { verifyCursor } from '../crypto/checkpoint.mjs';
import { encodeB64Url } from '../crypto/keys.js';

function eventRow(r) {
  return {
    deviceId: r.device_id,
    sequence: Number(r.sequence),
    digest: r.digest,
    eventId: r.event_id,
    occurredAt: r.occurred_at,
    keyVersion: Number(r.key_version),
    prevDigest: r.prev_digest,
    payload: r.payload,
    signature: encodeB64Url(r.signature),
  };
}

export async function getCheckpointPayload(pool, serverKey, deviceId, sequence) {
  const { rows } = await pool.query(
    `SELECT sequence, digest, prev_checkpoint_digest, generated_at, signature
       FROM checkpoints WHERE device_id=$1 AND sequence=$2`,
    [deviceId, sequence]
  );
  if (rows.length === 0) throw errors.internal('checkpoint row missing');
  const r = rows[0];
  return {
    deviceId,
    sequence: Number(r.sequence),
    digest: r.digest,
    prevCheckpointDigest: r.prev_checkpoint_digest,
    generatedAt: r.generated_at.toISOString(),
    signature: encodeB64Url(r.signature),
    signerPublicKey: serverKey.publicB64Url,
  };
}

/**
 * Open (or continue) a fixed view and fetch one page.
 * @param afterSequence null/0 to start a new view; otherwise a signed cursor
 */
export async function readPage(pool, serverKey, cfg, params) {
  const { deviceId, limit, cursorToken, explicitAfterSequence } = params;

  let view;
  let afterSequence;

  if (cursorToken) {
    const parsed = verifyCursor(serverKey, cursorToken);
    if (!parsed) throw errors.validation('invalid or tampered page cursor');
    if (parsed.deviceId !== deviceId) {
      throw errors.validation('cursor was issued for a different device', {
        cursorDeviceId: parsed.deviceId,
        deviceId,
      });
    }
    if (explicitAfterSequence !== undefined && explicitAfterSequence !== parsed.afterSequence) {
      throw errors.validation('afterSequence contradicts the cursor position', {
        cursorAfterSequence: parsed.afterSequence,
        afterSequence: explicitAfterSequence,
      });
    }
    afterSequence = parsed.afterSequence;

    const { rows } = await pool.query(
      `SELECT view_id, device_id, high_watermark, start_sequence, expires_at
         FROM read_views WHERE view_id=$1 AND device_id=$2`,
      [parsed.viewId, deviceId]
    );
    if (rows.length === 0) throw errors.gone('read view no longer exists');
    view = rows[0];
  } else {
    // First page: create the fixed snapshot.
    afterSequence = explicitAfterSequence ?? 0;
    if (!Number.isInteger(afterSequence) || afterSequence < 0) {
      throw errors.validation('afterSequence must be a non-negative integer');
    }
    view = await withTransaction(pool, async (client) => {
      const dev = await client.query(
        'SELECT high_watermark FROM devices WHERE device_id=$1 FOR SHARE',
        [deviceId]
      );
      if (dev.rows.length === 0) throw errors.deviceNotFound(deviceId);
      const hwm = Number(dev.rows[0].high_watermark);
      const viewId = randomUUID();
      const expiresAt = new Date(Date.now() + cfg.compaction.viewTtlMs);
      await client.query(
        `INSERT INTO read_views (view_id, device_id, high_watermark, start_sequence, expires_at)
         VALUES ($1,$2,$3,$4,$5)`,
        [viewId, deviceId, hwm, afterSequence, expiresAt]
      );
      return { view_id: viewId, device_id: deviceId, high_watermark: hwm, start_sequence: afterSequence };
    });
    if (afterSequence > Number(view.high_watermark)) {
      throw errors.validation('afterSequence is beyond the current high watermark', {
        afterSequence,
        viewHighWatermark: Number(view.high_watermark),
      });
    }
  }

  const viewHwm = Number(view.high_watermark);
  const pageSize = Math.min(Math.max(1, limit || cfg.defaultPageSize), cfg.maxPageSize);

  // Latest checkpoint: events at or below it may have been deleted.
  const { rows: cpRows } = await pool.query(
    'SELECT sequence FROM checkpoints WHERE device_id=$1 ORDER BY sequence DESC LIMIT 1',
    [deviceId]
  );
  const checkpointSeq = cpRows.length ? Number(cpRows[0].sequence) : 0;

  const expired = cursorToken && Date.parse(view.expires_at) < Date.now();

  if (afterSequence < checkpointSeq) {
    // The requested position is behind a checkpoint. Return 410 with a
    // verifiable recovery point rather than silently skipping auditable data.
    const cp = await getCheckpointPayload(pool, serverKey, deviceId, checkpointSeq);
    throw errors.gone(
      `events through sequence ${checkpointSeq} have been compacted into a checkpoint; ` +
        'verify the checkpoint signature and resume from checkpoint.sequence + 1',
      { checkpoint: cp, resumeFromSequence: checkpointSeq + 1 }
    );
  }

  if (expired) {
    throw errors.gone('read view has expired; open a new view');
  }

  if (afterSequence > viewHwm) {
    throw errors.validation('cursor position is beyond the view watermark');
  }

  const { rows } = await pool.query(
    `SELECT device_id, sequence, digest, event_id, occurred_at, key_version,
            prev_digest, payload, signature
       FROM event_records
      WHERE device_id=$1
        AND status='visible'
        AND sequence > $2
        AND sequence <= $3
      ORDER BY sequence
      LIMIT $4`,
    [deviceId, afterSequence, viewHwm, pageSize]
  );
  const events = rows.map(eventRow);
  const lastSeq = events.length ? Number(events[events.length - 1].sequence) : afterSequence;
  const nextCursor = lastSeq < viewHwm
    ? signCursor(serverKey, { viewId: view.view_id, deviceId, afterSequence: lastSeq })
    : null;

  return {
    deviceId,
    viewId: view.view_id,
    viewHighWatermark: viewHwm,
    fromSequence: afterSequence + 1,
    events,
    nextCursor,
    hasMore: nextCursor !== null,
  };
}

/**
 * Long-poll for new consecutive events.
 * @param signal AbortSignal from the HTTP request (client disconnect)
 */
export async function waitForEvents(pool, serverKey, cfg, { deviceId, afterSequence, limit, signal }) {
  if (!Number.isInteger(afterSequence) || afterSequence < 0) {
    throw errors.validation('afterSequence must be a non-negative integer');
  }
  const pageSize = Math.min(Math.max(1, limit || cfg.defaultPageSize), cfg.maxPageSize);
  const maxWait = Math.min(cfg.waitTimeoutMs, 30_000);

  // Dedicated client: LISTEN state is per-connection. Released on every exit
  // path (including abort), so a disconnecting client never leaks a waiter or
  // a database connection.
  const client = await pool.connect();

  // One consistent state snapshot: latest checkpoint, watermark and the event
  // batch are read in one transaction holding FOR SHARE on the device row.
  // Compaction and ingest both take FOR UPDATE on that row, so:
  //  * a checkpoint committed before the lock is visible here and yields 410;
  //  * a compaction starting while the lock is held blocks until we COMMIT, so
  //    the checkpoint-insert/event-delete can never be observed half-applied;
  //  * events returned are the whole visible tail past afterSequence at one
  //    point in time - a 200 can never skip a sequence a checkpoint covers.
  const snapshot = async () => withClientTransaction(client, async (tx) => {
    const dev = await tx.query(
      'SELECT high_watermark FROM devices WHERE device_id=$1 FOR SHARE',
      [deviceId]
    );
    if (dev.rows.length === 0) throw errors.deviceNotFound(deviceId);
    const hwm = Number(dev.rows[0].high_watermark);

    const cp = await tx.query(
      'SELECT sequence FROM checkpoints WHERE device_id=$1 ORDER BY sequence DESC LIMIT 1',
      [deviceId]
    );
    const checkpointSeq = cp.rows.length ? Number(cp.rows[0].sequence) : 0;

    if (afterSequence < checkpointSeq) {
      // Requested position is behind a checkpoint: surface the same verifiable
      // recovery point as a paginated read instead of silently skipping the
      // compacted auditable prefix.
      const checkpoint = await getCheckpointPayload(tx, serverKey, deviceId, checkpointSeq);
      throw errors.gone(
        `events through sequence ${checkpointSeq} have been compacted into a checkpoint; ` +
          'verify the checkpoint signature and resume from checkpoint.sequence + 1',
        { checkpoint, resumeFromSequence: checkpointSeq + 1 }
      );
    }

    let events = [];
    if (hwm > afterSequence) {
      const { rows } = await tx.query(
        `SELECT device_id, sequence, digest, event_id, occurred_at, key_version,
                prev_digest, payload, signature
           FROM event_records
          WHERE device_id=$1 AND status='visible' AND sequence > $2
          ORDER BY sequence LIMIT $3`,
        [deviceId, afterSequence, pageSize]
      );
      events = rows.map(eventRow);
    }
    return { hwm, events };
  });

  // The handler stays attached for the ENTIRE request (including while
  // snapshots run), so a notification committed in any window can never be
  // emitted with no listener and lost.
  let pendingWake = false;
  let wakeWaiter = null;
  const onNotification = (msg) => {
    if (msg.payload !== deviceId) return;
    if (msg.channel !== 'telemetry_events' && msg.channel !== 'telemetry_compacted') return;
    pendingWake = true;
    if (wakeWaiter) {
      const w = wakeWaiter;
      wakeWaiter = null;
      w();
    }
  };

  try {
    // Listen on BOTH channels before the first snapshot:
    //  * telemetry_events    - additive watermark advancement (ingest etc.)
    //  * telemetry_compacted - a checkpoint was inserted (prefix may be gone)
    // A commit landing after this point is either already visible in a
    // snapshot or queues a notification; NOTIFY is delivered only at COMMIT,
    // so an aborted compaction never causes a spurious wake.
    await client.query('LISTEN telemetry_events');
    await client.query('LISTEN telemetry_compacted');
    client.on('notification', onNotification);

    const awaitWakeup = (remainingMs) => new Promise((resolve) => {
      // A notification could have arrived while a snapshot was running.
      if (pendingWake) return resolve('notified');
      let settled = false;
      const done = (reason) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener('abort', onAbort);
        resolve(reason);
      };
      const onAbort = () => done('aborted');
      const timer = setTimeout(() => done('timeout'), remainingMs);
      signal?.addEventListener('abort', onAbort, { once: true });
      wakeWaiter = () => done('notified');
    });

    const deadline = Date.now() + maxWait;
    let { hwm, events } = await snapshot();

    while (events.length === 0) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;
      const reason = await awaitWakeup(remaining);
      if (reason === 'aborted' || signal?.aborted) {
        const e = new Error('client closed request');
        e.code = 'ABORTED';
        throw e;
      }
      // Re-snapshot under the device lock on every wakeup: a normal commit
      // yields its new events, while a compaction committed while parked is
      // visible here and produces 410. A notification whose commit lands after
      // this snapshot either blocks on our FOR SHARE (so it cannot delete the
      // events we just read) or remains pending and drives another snapshot.
      pendingWake = false;
      ({ hwm, events } = await snapshot());
    }

    return {
      deviceId,
      highWatermark: hwm,
      afterSequence,
      timeout: events.length === 0,
      events,
    };
  } finally {
    client.removeListener('notification', onNotification);
    try {
      await client.query('UNLISTEN telemetry_events');
      await client.query('UNLISTEN telemetry_compacted');
    } catch {
      // connection may already be gone
    }
    client.release();
  }
}
