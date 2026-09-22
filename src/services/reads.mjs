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
// Waiting: /wait uses LISTEN/NOTIFY. The order is LISTEN -> read state ->
// block, so a notification produced between the check and the wait cannot be
// lost. Both try_advance (ingest) and the checkpoints INSERT trigger fire on
// the same channel, so a compaction that lands while a waiter is blocked wakes
// it immediately. On timeout an empty page is returned. Client disconnect
// releases the dedicated connection back to the pool immediately.
//
// Recovery semantics: like the paginated read, /wait must never return a gapped
// tail after history compaction. Whenever afterSequence is behind the latest
// checkpoint it returns HTTP 410 GONE carrying the verifiable checkpoint and
// resumeFromSequence. The check runs before every read, after every wakeup
// (including timeout), and again right after fetching events, so a compaction
// committing concurrently with the request can never cross the checkpoint with
// an HTTP 200.

import { randomUUID } from 'node:crypto';
import { withTransaction } from '../db/pool.mjs';
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
 * Read the device watermark and the latest checkpoint sequence in one query.
 * Rows committed by a concurrent transaction only ever become visible together,
 * so the two values are mutually consistent at each read.
 */
async function readWaitState(client, deviceId) {
  const { rows } = await client.query(
    `SELECT d.high_watermark AS hwm,
            (SELECT c.sequence FROM checkpoints c
              WHERE c.device_id = d.device_id
              ORDER BY c.sequence DESC LIMIT 1) AS checkpoint_seq
       FROM devices d WHERE d.device_id=$1`,
    [deviceId]
  );
  if (rows.length === 0) return null;
  return { hwm: Number(rows[0].hwm), checkpointSeq: rows[0].checkpoint_seq ? Number(rows[0].checkpoint_seq) : 0 };
}

/**
 * Long-poll for new consecutive events.
 *
 * Recovery mirrors {@link readPage}: if afterSequence is at or behind a
 * checkpoint the compacted events can no longer be delivered, so the call
 * rejects with HTTP 410 GONE carrying the checkpoint and resumeFromSequence
 * instead of returning a discontinuous tail.
 *
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
  try {
    await client.query('LISTEN telemetry_events');

    const readBatch = async (from) => {
      const { rows } = await client.query(
        `SELECT device_id, sequence, digest, event_id, occurred_at, key_version,
                prev_digest, payload, signature
           FROM event_records
          WHERE device_id=$1 AND status='visible' AND sequence > $2
          ORDER BY sequence LIMIT $3`,
        [deviceId, from, pageSize]
      );
      return rows.map(eventRow);
    };

    // 410 with the same recovery payload a paginated read would return, so a
    // caller has one recovery path for both read styles.
    const checkpointGone = async (checkpointSeq) => {
      const cp = await getCheckpointPayload(pool, serverKey, deviceId, checkpointSeq);
      throw errors.gone(
        `events through sequence ${checkpointSeq} have been compacted into a checkpoint; ` +
          'verify the checkpoint signature and resume from checkpoint.sequence + 1',
        { checkpoint: cp, resumeFromSequence: checkpointSeq + 1 }
      );
    };

    let timedOut = false;
    let hwm = 0;
    // One deadline for the whole request: a wakeup that does not yield events
    // (a notification unrelated to advancement, or a checkpoint that does not
    // move this caller behind it) never extends the advertised wait window.
    const deadline = Date.now() + maxWait;

    // Re-evaluated from scratch after every wakeup: a compaction that commits
    // during the wait may have deleted the events this call is waiting behind,
    // and new ingests may make events available. LISTEN happened BEFORE the
    // first read, so any committing advancement either was already visible here
    // or has its NOTIFY queued for the blocking wait below (no lost wakeup).
    for (;;) {
      const state = await readWaitState(client, deviceId);
      if (state === null) throw errors.deviceNotFound(deviceId);
      hwm = state.hwm;

      if (afterSequence < state.checkpointSeq) {
        await checkpointGone(state.checkpointSeq);
      }

      if (hwm > afterSequence) {
        const events = await readBatch(afterSequence);
        // Guard against a compaction that committed between the state read and
        // the event read: the checkpoint table and the covered event rows change
        // in one transaction, so either those rows were still visible here, or
        // the checkpoint row now exists and this re-read must observe it.
        const { rows: cpRows } = await client.query(
          'SELECT sequence FROM checkpoints WHERE device_id=$1 AND sequence > $2 ORDER BY sequence DESC LIMIT 1',
          [deviceId, afterSequence]
        );
        if (cpRows.length) {
          await checkpointGone(Number(cpRows[0].sequence));
        }
        return {
          deviceId,
          highWatermark: hwm,
          afterSequence,
          timeout: false,
          events,
        };
      }

      if (timedOut) {
        return {
          deviceId,
          highWatermark: hwm,
          afterSequence,
          timeout: true,
          events: [],
        };
      }

      await new Promise((resolve) => {
        let settled = false;
        let timer = null;
        const finish = () => {
          if (settled) return;
          settled = true;
          if (timer) clearTimeout(timer);
          signal?.removeEventListener('abort', onAbort);
          client.removeListener('notification', onNotification);
          resolve();
        };
        const onNotification = (msg) => {
          if (msg.channel === 'telemetry_events' && msg.payload === deviceId) finish();
        };
        const onAbort = () => finish();
        // node-postgres keeps parsing the socket stream while the connection
        // is idle, so NOTIFY arrives immediately with no polling query.
        client.on('notification', onNotification);
        signal?.addEventListener('abort', onAbort, { once: true });
        timer = setTimeout(() => { timedOut = true; finish(); }, Math.max(0, deadline - Date.now()));
      });

      if (signal?.aborted) {
        const e = new Error('client closed request');
        e.code = 'ABORTED';
        throw e;
      }
    }
  } finally {
    try {
      await client.query('UNLISTEN telemetry_events');
    } catch {
      // connection may already be gone
    }
    client.release();
  }
}
