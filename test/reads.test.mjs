'use strict';
// Fixed-view pagination, cursor integrity and long-poll waiting.

import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { setupHarness, stopTestPostgres, truncateAll } from './helpers/harness.mjs';
import { registerDevice } from '../src/services/devices.mjs';
import { ingestBatch } from '../src/services/ingest.mjs';
import { readPage, waitForEvents } from '../src/services/reads.mjs';
import { compactDevice } from '../src/services/compaction.mjs';
import { DeviceSigner, buildChain, prepareBatch, buildEnvelope } from './helpers/events.mjs';
import { signCursor } from '../src/crypto/checkpoint.mjs';
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

async function buildVisible(id, n) {
  const signer = new DeviceSigner();
  await registerDevice(h.pool, { deviceId: id, publicKeyRaw: signer.publicRaw });
  const chain = buildChain(signer, id, n);
  await ingest(id, 'fill', chain);
  return { signer, chain };
}

test('wait behind an existing checkpoint returns 410 with a verifiable recovery point', async () => {
  // The reported defect: sequences 1..4 compacted to cutoff 2 while /wait with
  // afterSequence=0 silently returned only 3,4 as HTTP 200.
  const id = 'dev-wait-410-existing';
  const { chain } = await buildVisible(id, 4);
  const out = await compactDevice(h.pool, h.serverKey, h.cfg, id, 2, 'cmd-wait-410-1');
  assert.equal(out.checkpoint.sequence, 2);

  const err = await waitForEvents(
    h.pool, h.serverKey, h.cfg,
    { deviceId: id, afterSequence: 0, limit: 10, signal: new AbortController().signal }
  ).then(() => null, (e) => e);
  assert.ok(err instanceof ApiError && err.status === 410, `expected 410, got ${err?.status}`);
  assert.equal(err.details.resumeFromSequence, 3);
  const cp = err.details.checkpoint;
  assert.equal(cp.sequence, 2);
  assert.equal(cp.deviceId, id);
  assert.equal(cp.digest, buildEnvelope(chain[1]).digest);
  assert.equal(cp.signerPublicKey, h.serverKey.publicB64Url);
  assert.ok(typeof cp.signature === 'string' && cp.signature.length > 0);

  // After verifying the checkpoint, anchoring afterSequence at its sequence
  // delivers exactly the retained tail, consistent with a paginated read.
  const resumed = await waitForEvents(
    h.pool, h.serverKey, h.cfg,
    { deviceId: id, afterSequence: 2, limit: 10, signal: new AbortController().signal }
  );
  assert.equal(resumed.timeout, false);
  assert.deepEqual(resumed.events.map((e) => e.sequence), [3, 4]);
});

test('wait at/after the checkpoint stays normal: exactly-at and ahead are not 410', async () => {
  const id = 'dev-wait-410-edge';
  await buildVisible(id, 4);
  await compactDevice(h.pool, h.serverKey, h.cfg, id, 2, 'cmd-wait-410-edge');

  // afterSequence == checkpoint.sequence is the verified anchor, not behind it.
  const at = await waitForEvents(
    h.pool, h.serverKey, h.cfg,
    { deviceId: id, afterSequence: 2, limit: 10, signal: new AbortController().signal }
  );
  assert.deepEqual(at.events.map((e) => e.sequence), [3, 4]);

  // afterSequence ahead of the checkpoint is likewise unaffected.
  const ahead = await waitForEvents(
    h.pool, h.serverKey, h.cfg,
    { deviceId: id, afterSequence: 3, limit: 10, signal: new AbortController().signal }
  );
  assert.deepEqual(ahead.events.map((e) => e.sequence), [4]);
});

test('compaction notifies the wait channel so blocked waiters wake up', async () => {
  // The checkpoints INSERT trigger is the only signal a blocked waiter gets
  // when history is compacted without any watermark movement. Verify the
  // NOTIFY directly over a raw LISTEN connection.
  const id = 'dev-wait-notify';
  await buildVisible(id, 4);

  const listener = await h.pool.connect();
  const notifications = [];
  await listener.query('LISTEN telemetry_events');
  listener.on('notification', (msg) => {
    if (msg.channel === 'telemetry_events') notifications.push(msg.payload);
  });
  try {
    await compactDevice(h.pool, h.serverKey, h.cfg, id, 2, 'cmd-wait-notify');
    // NOTIFY is delivered at commit; drain the socket.
    await new Promise((r) => setTimeout(r, 300));
    assert.ok(notifications.includes(id), `expected NOTIFY for ${id}, got ${JSON.stringify(notifications)}`);
  } finally {
    await listener.query('UNLISTEN telemetry_events').catch(() => {});
    listener.removeAllListeners('notification');
    listener.release();
  }
});

test('ingest + compaction racing a blocked waiter never returns a gapped 200', async () => {
  // Physical scenario for "compaction lands while waiting": the waiter blocks
  // with hwm == afterSequence; events then commit (which wakes it) and history
  // is compacted to cutoff 2 in the same window. Every outcome must be either a
  // complete consecutive 200 batch or the 410 recovery - never only 3,4.
  for (let i = 0; i < 12; i++) {
    const dev = `dev-wait-race-${i}`;
    const signer = new DeviceSigner();
    await registerDevice(h.pool, { deviceId: dev, publicKeyRaw: signer.publicRaw });

    const ac = new AbortController();
    const p = waitForEvents(h.pool, h.serverKey, h.cfg, {
      deviceId: dev, afterSequence: 0, limit: 10, signal: ac.signal,
    });
    await new Promise((r) => setTimeout(r, 150)); // LISTEN established, blocked
    await ingest(dev, `fill-${i}`, buildChain(signer, dev, 4));
    // Compact immediately: contends with the waiter's post-wake re-read.
    const compactP = compactDevice(h.pool, h.serverKey, h.cfg, dev, 2, `cmd-wait-race-${i}`);
    const outcome = await p.then(
      (value) => ({ status: 'fulfilled', value }),
      (reason) => ({ status: 'rejected', reason })
    );
    await compactP;
    if (outcome.status === 'fulfilled') {
      assert.deepEqual(
        outcome.value.events.map((e) => e.sequence),
        [1, 2, 3, 4],
        `iteration ${i}: a 200 must carry the complete consecutive prefix`
      );
    } else {
      assert.equal(outcome.reason?.status, 410, `iteration ${i}: unexpected rejection`);
      assert.equal(outcome.reason.details?.resumeFromSequence, 3);
    }
  }
});

test('compaction committing against an in-flight event read cannot yield a gapped 200', async () => {
  // Force the exact interleaving the post-read guard exists for: the waiter's
  // event SELECT and the compaction transaction queue against one held table
  // lock and are released together, so compaction can commit around the read.
  const id = 'dev-wait-410-readlock';
  await buildVisible(id, 4);

  const locker = await h.pool.connect();
  let lockCommitted = false;
  try {
    await locker.query('BEGIN');
    await locker.query('LOCK TABLE event_records IN ACCESS EXCLUSIVE MODE');

    const ac = new AbortController();
    const waiterP = waitForEvents(h.pool, h.serverKey, h.cfg, {
      deviceId: id, afterSequence: 0, limit: 10, signal: ac.signal,
    });
    // readWaitState (devices/checkpoints) proceeds; the event SELECT queues
    // behind the table lock.
    await new Promise((r) => setTimeout(r, 200));
    const compactP = compactDevice(h.pool, h.serverKey, h.cfg, id, 2, 'cmd-wait-readlock');
    await new Promise((r) => setTimeout(r, 100));
    await locker.query('COMMIT');
    lockCommitted = true;

    const outcome = await waiterP.then(
      (value) => ({ status: 'fulfilled', value }),
      (reason) => ({ status: 'rejected', reason })
    );
    await compactP;
    if (outcome.status === 'fulfilled') {
      assert.deepEqual(
        outcome.value.events.map((e) => e.sequence),
        [1, 2, 3, 4],
        'a 200 racing compaction must still carry the complete consecutive prefix'
      );
    } else {
      assert.equal(outcome.reason?.status, 410);
      assert.equal(outcome.reason.details?.resumeFromSequence, 3);
    }
  } finally {
    if (!lockCommitted) await locker.query('ROLLBACK').catch(() => {});
    locker.release();
  }
});
