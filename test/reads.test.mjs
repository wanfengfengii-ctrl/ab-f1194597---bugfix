'use strict';
// Fixed-view pagination, cursor integrity and long-poll waiting.

import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setupHarness, stopTestPostgres, truncateAll } from './helpers/harness.mjs';
import { registerDevice } from '../src/services/devices.mjs';
import { ingestBatch } from '../src/services/ingest.mjs';
import { readPage, waitForEvents } from '../src/services/reads.mjs';
import { compactDevice } from '../src/services/compaction.mjs';
import { DeviceSigner, buildChain, prepareBatch } from './helpers/events.mjs';
import { signCursor, canonicalCheckpoint } from '../src/crypto/checkpoint.mjs';
import { decodeB64Url, rawToPublicKeyObject, verifyBytes } from '../src/crypto/keys.js';
import { ApiError } from '../src/errors.mjs';

let h;
before(async () => { h = await setupHarness(); });
after(async () => { await h.pool.end(); await stopTestPostgres(); });
beforeEach(async () => { await truncateAll(h.pool); });

const ingest = (id, rid, evs) => ingestBatch(h.pool, prepareBatch(id, rid, evs));
const readFirst = (id, limit) =>
  readPage(h.pool, h.serverKey, h.cfg, { deviceId: id, limit, cursorToken: null, explicitAfterSequence: undefined });

test('view is fixed: commits after the first page never mix in', async () => {
  const id = 'dev-view';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  await ingest(id, 'a', buildChain(signer, id, 3));

  const first = await readFirst(id, 1);
  assert.equal(first.viewHighWatermark, 3);
  assert.equal(first.events.length, 1);
  const { viewId } = first;

  // New commits arrive while the client pages through the old view.
  await ingest(id, 'b', buildChain(signer, id, 5).slice(3));

  const second = await readPage(h.pool, h.serverKey, h.cfg, {
    deviceId: id, limit: 5, cursorToken: first.nextCursor, explicitAfterSequence: undefined,
  });
  assert.equal(second.viewId, viewId);
  assert.equal(second.viewHighWatermark, 3);
  assert.deepEqual(second.events.map((e) => e.sequence), [2, 3]);
  assert.equal(second.nextCursor, null);

  // A fresh first page observes the new watermark.
  const fresh = await readFirst(id, 10);
  assert.equal(fresh.viewHighWatermark, 5);
});

test('tampered, cross-device and contradictory cursors are rejected', async () => {
  const a = 'dev-cur-a';
  const b = 'dev-cur-b';
  const s1 = new DeviceSigner();
  const s2 = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: a, publicKeyRaw: s1.publicRaw });
  await registerDevice(h.pool, { deviceId: b, publicKeyRaw: s2.publicRaw });
  await ingest(a, 'a', buildChain(s1, a, 2));
  await ingest(b, 'b', buildChain(s2, b, 2));

  const page = await readFirst(a, 1);
  const cursor = page.nextCursor;

  // Tamper with the body.
  const [body, sig] = cursor.split('.');
  const tampered = body.replace(/.$/, (c) => (c === 'A' ? 'B' : 'A')) + '.' + sig;
  await assert.rejects(
    () => readPage(h.pool, h.serverKey, h.cfg, { deviceId: a, cursorToken: tampered, explicitAfterSequence: undefined }),
    (e) => e.status === 400
  );

  // Forge a cursor for another view/device but signed by nobody valid.
  const forged = signCursor(h.serverKey, { viewId: page.viewId, deviceId: b, afterSequence: 0 });
  await assert.rejects(
    () => readPage(h.pool, h.serverKey, h.cfg, { deviceId: a, cursorToken: forged, explicitAfterSequence: undefined }),
    (e) => e.status === 400
  );

  // Correctly signed cursor for device b bound to a's (nonexistent there) view.
  const cross = signCursor(h.serverKey, { viewId: page.viewId, deviceId: b, afterSequence: 0 });
  await assert.rejects(
    () => readPage(h.pool, h.serverKey, h.cfg, { deviceId: b, cursorToken: cross, explicitAfterSequence: undefined }),
    (e) => e.status === 410 || e.status === 400
  );

  // afterSequence contradicting the cursor position.
  await assert.rejects(
    () => readPage(h.pool, h.serverKey, h.cfg, { deviceId: a, cursorToken: cursor, explicitAfterSequence: 99 }),
    (e) => e.status === 400
  );

  // Valid continuation works.
  const ok = await readPage(h.pool, h.serverKey, h.cfg, { deviceId: a, cursorToken: cursor, explicitAfterSequence: undefined });
  assert.equal(ok.events[0].sequence, 2);
});

test('wait returns immediately when events already exist', async () => {
  const id = 'dev-wait-now';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  await ingest(id, 'a', buildChain(signer, id, 2));
  const ac = new AbortController();
  const t0 = Date.now();
  const res = await waitForEvents(h.pool, h.serverKey, h.cfg, { deviceId: id, afterSequence: 0, limit: 10, signal: ac.signal });
  assert.ok(Date.now() - t0 < 1000);
  assert.equal(res.events.length, 2);
  assert.equal(res.timeout, false);
});

test('wait wakes on a new commit and does not miss the notification', async () => {
  const id = 'dev-wait-later';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });

  const ac = new AbortController();
  const waiter = waitForEvents(h.pool, h.serverKey, h.cfg, {
    deviceId: id, afterSequence: 0, limit: 10, signal: ac.signal,
  });
  // Commit after the waiter has begun listening (small delay to ensure LISTEN).
  setTimeout(() => ingest(id, 'a', buildChain(signer, id, 1)), 150);
  const res = await waiter;
  assert.equal(res.timeout, false);
  assert.equal(res.events.length, 1);
  assert.equal(res.highWatermark, 1);
});

test('wait timeout returns an empty result', async () => {
  const id = 'dev-wait-timeout';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  const cfg = { ...h.cfg, waitTimeoutMs: 400 };
  const ac = new AbortController();
  const res = await waitForEvents(h.pool, h.serverKey, cfg, { deviceId: id, afterSequence: 0, limit: 10, signal: ac.signal });
  assert.equal(res.timeout, true);
  assert.equal(res.events.length, 0);
});

test('aborting the wait releases it (client disconnect)', async () => {
  const id = 'dev-wait-abort';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  const ac = new AbortController();
  const p = waitForEvents(h.pool, h.serverKey, h.cfg, { deviceId: id, afterSequence: 0, limit: 10, signal: ac.signal });
  setTimeout(() => ac.abort(), 100);
  await assert.rejects(p, (e) => e.code === 'ABORTED');
  // Pool stays usable after an aborted wait.
  const { rows } = await h.pool.query('SELECT 1 AS ok');
  assert.equal(rows[0].ok, 1);
});

// --- Checkpoint recovery semantics for /wait ------------------------------
// A long poll must behave exactly like a paginated read when its position is
// behind a checkpoint: HTTP 410 GONE carrying the verifiable checkpoint and
// resumeFromSequence, never a 200 whose events silently skip the deleted
// prefix.

test('wait from the history start behind a checkpoint gets 410, then resumes at the checkpoint', async () => {
  const id = 'dev-wait-410';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  await ingest(id, 'r1', buildChain(signer, id, 4));
  // Background threshold is irrelevant: a manual checkpoint through seq 2.
  const cp = await compactDevice(h.pool, h.serverKey, h.cfg, id, 2, randomUUID());
  assert.equal(cp.checkpoint.sequence, 2);

  const ac = new AbortController();
  const err = await waitForEvents(h.pool, h.serverKey, h.cfg, {
    deviceId: id, afterSequence: 0, limit: 10, signal: ac.signal,
  }).then(() => null, (e) => e);
  assert.ok(err instanceof ApiError && err.status === 410, `expected 410, got ${err?.status}`);
  assert.equal(err.details.resumeFromSequence, 3);
  const got = err.details.checkpoint;
  assert.equal(got.sequence, 2);
  assert.equal(got.deviceId, id);

  // The recovery point is independently verifiable with the server public key.
  const { bytes } = canonicalCheckpoint(got);
  assert.ok(verifyBytes(rawToPublicKeyObject(h.serverKey.publicRaw), bytes, decodeB64Url(got.signature)));

  // Caller verifies, anchors at checkpoint.sequence and receives only the tail.
  const resumed = await waitForEvents(h.pool, h.serverKey, h.cfg, {
    deviceId: id, afterSequence: 2, limit: 10, signal: new AbortController().signal,
  });
  assert.equal(resumed.timeout, false);
  assert.deepEqual(resumed.events.map((e) => e.sequence), [3, 4]);

  // /wait and paginated /events agree on the resumed view.
  const page = await readPage(h.pool, h.serverKey, h.cfg, {
    deviceId: id, limit: 10, cursorToken: null, explicitAfterSequence: 2,
  });
  assert.deepEqual(page.events.map((e) => e.sequence), [3, 4]);
});

test('wait at or after the checkpoint sequence is not GONE', async () => {
  const id = 'dev-wait-at-cp';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  await ingest(id, 'r1', buildChain(signer, id, 4));
  await compactDevice(h.pool, h.serverKey, h.cfg, id, 2, randomUUID());

  const at = await waitForEvents(h.pool, h.serverKey, h.cfg, {
    deviceId: id, afterSequence: 2, limit: 10, signal: new AbortController().signal,
  });
  assert.deepEqual(at.events.map((e) => e.sequence), [3, 4]);

  const past = await waitForEvents(h.pool, h.serverKey, h.cfg, {
    deviceId: id, afterSequence: 3, limit: 10, signal: new AbortController().signal,
  });
  assert.deepEqual(past.events.map((e) => e.sequence), [4]);
});

test('ingest plus compaction while parked yields 410 or the complete prefix, never a tail-only 200', async () => {
  // A waiter parks on a fresh device (afterSequence == highWatermark == 0).
  // Events commit and a checkpoint crossing its position follows. Depending on
  // lock scheduling the post-wakeup snapshot lands before or after the
  // checkpoint: both are correct (full prefix 200 vs. 410 recovery). The one
  // forbidden outcome is a 200 containing only the retained tail.
  for (let i = 0; i < 8; i++) {
    const id = `dev-wait-parked-${i}`;
    const signer = new DeviceSigner();
    await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });

    const ac = new AbortController();
    const p = waitForEvents(h.pool, h.serverKey, h.cfg, {
      deviceId: id, afterSequence: 0, limit: 10, signal: ac.signal,
    });
    setTimeout(async () => {
      await ingest(id, randomUUID(), buildChain(signer, id, 4));
      await compactDevice(h.pool, h.serverKey, h.cfg, id, 2, randomUUID()).catch(() => {});
    }, 100);

    const outcome = await p.then((v) => ({ kind: 'ok', v }), (e) => ({ kind: 'err', e }));
    if (outcome.kind === 'err') {
      assert.ok(outcome.e instanceof ApiError && outcome.e.status === 410,
        `iteration ${i}: unexpected error ${outcome.e?.status}`);
      assert.equal(outcome.e.details.resumeFromSequence, 3);
      assert.equal(outcome.e.details.checkpoint.sequence, 2);
    } else {
      assert.deepEqual(outcome.v.events.map((e) => e.sequence), [1, 2, 3, 4],
        `iteration ${i}: wait returned a discontinuous tail`);
    }
  }
});

test('checkpoint commit notifies parked waiters on the compaction channel', async () => {
  // Directly prove migration 0004: inserting a checkpoint NOTIFYes
  // telemetry_compacted, so a parked /wait (whose only wakeup source would
  // otherwise be ingest) can re-evaluate against the new checkpoint.
  const id = 'dev-wait-notify';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  await ingest(id, 'r1', buildChain(signer, id, 4));

  const listener = await h.pool.connect();
  await listener.query('LISTEN telemetry_compacted');
  const notified = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('no compaction notification received')), 3000);
    listener.on('notification', (msg) => {
      clearTimeout(timer);
      resolve(msg);
    });
  });
  try {
    await compactDevice(h.pool, h.serverKey, h.cfg, id, 2, randomUUID());
    const msg = await notified;
    assert.equal(msg.channel, 'telemetry_compacted');
    assert.equal(msg.payload, id);
  } finally {
    await listener.query('UNLISTEN telemetry_compacted');
    listener.release();
  }
});

test('compaction queued ahead of the post-wakeup re-read makes a parked wait return 410', async () => {
  // Deterministic variant of "compaction while waiting": a blocker holds the
  // device row lock; ingest and compaction queue behind it; the parked waiter
  // is woken by the ingest NOTIFY when the blocker releases. Because
  // compaction's FOR UPDATE is already queued ahead of the waiter's next FOR
  // SHARE snapshot, that snapshot must observe the checkpoint and yield 410
  // instead of racing out a tail-only 200.
  const id = 'dev-wait-queued-cp';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });

  const blocker = await h.pool.connect();
  await blocker.query('BEGIN');
  await blocker.query('SELECT high_watermark FROM devices WHERE device_id=$1 FOR UPDATE', [id]);

  const p = waitForEvents(h.pool, h.serverKey, h.cfg, {
    deviceId: id, afterSequence: 0, limit: 10, signal: new AbortController().signal,
  });
  await new Promise((r) => setTimeout(r, 100));

  const ingestDone = ingest(id, randomUUID(), buildChain(signer, id, 4));
  const compactDone = compactDevice(h.pool, h.serverKey, h.cfg, id, 2, randomUUID())
    .then((v) => v, (e) => ({ err: e.status }));
  await new Promise((r) => setTimeout(r, 100));
  await blocker.query('COMMIT');
  blocker.release();

  const err = await p.then(() => null, (e) => e);
  await ingestDone;
  const cpOut = await compactDone;
  assert.equal(cpOut.err, undefined);
  assert.ok(err instanceof ApiError && err.status === 410, `expected 410, got ${err?.status}`);
  assert.equal(err.details.checkpoint.sequence, 2);
  assert.equal(err.details.resumeFromSequence, 3);
});

test('checkpoint exactly at the parked position wakes but does not GONE', async () => {  const id = 'dev-wait-cp-equal';
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  await ingest(id, 'r1', buildChain(signer, id, 4));

  const cfg = { ...h.cfg, waitTimeoutMs: 800 };
  const ac = new AbortController();
  const p = waitForEvents(h.pool, h.serverKey, cfg, {
    deviceId: id, afterSequence: 4, limit: 10, signal: ac.signal,
  });
  // Checkpoint covers exactly seq 4: the waiter needs no recovery point.
  setTimeout(() => compactDevice(h.pool, h.serverKey, h.cfg, id, 4, randomUUID()), 150);
  const res = await p;
  assert.equal(res.timeout, true);
  assert.deepEqual(res.events, []);
});

test('concurrent wait and compaction never yields a 200 that crosses the checkpoint', async () => {
  // Hammer the serialization boundary: the immediate wait and the compaction
  // race for the devices row lock. Every outcome is either a 200 containing
  // the FULL prefix from seq 1 or a 410 - a tail-only 200 is forbidden.
  for (let i = 0; i < 16; i++) {
    const id = `dev-wait-race-${i}`;
    const signer = new DeviceSigner();
    await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
    await ingest(id, randomUUID(), buildChain(signer, id, 4));

    const outcome = await Promise.allSettled([
      waitForEvents(h.pool, h.serverKey, h.cfg, {
        deviceId: id, afterSequence: 0, limit: 10, signal: new AbortController().signal,
      }),
      // Give the wait a head start to acquire its FOR SHARE snapshot.
      new Promise((r) => setTimeout(() => compactDevice(h.pool, h.serverKey, h.cfg, id, 2, randomUUID()).then(r, r), 5)),
    ]);

    const [waitResult] = outcome;
    if (waitResult.status === 'fulfilled') {
      const seqs = waitResult.value.events.map((e) => e.sequence);
      assert.deepEqual(seqs, [1, 2, 3, 4], `iteration ${i}: wait skipped events across checkpoint`);
    } else {
      const e = waitResult.reason;
      assert.ok(e instanceof ApiError && e.status === 410, `iteration ${i}: unexpected error ${e?.status}`);
      assert.equal(e.details.resumeFromSequence, 3);
    }
  }
});
