import assert from 'node:assert/strict';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { Worker } from 'node:worker_threads';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { SQLiteXNetStore } from '@xpell/xnet/server';
import { fixture, KEY, TYPE, rejects } from './fixtures.mjs';

function rows(f, table) {
  const db = new Database(f.options._db_path, { readonly: true });
  try { return db.prepare(`SELECT * FROM ${table}`).all(); } finally { db.close(); }
}
async function race(f, job, count = 6) {
  const workers = Array.from({ length: count }, (_, i) => new Worker(new URL('./worker.mjs', import.meta.url), {
    workerData: { options: { _db_path: f.options._db_path, _claim_key: KEY, _instance_id: f.instance_id,
      _limits: f.options._limits }, now: f.now, request_key: `worker-${i}`, ...job },
  }));
  const exits = workers.map(w => new Promise((resolve, reject) => { w.once('exit', code => code ? reject(new Error(`worker exit ${code}`)) : resolve()); w.once('error', reject); }));
  await Promise.all(workers.map(w => new Promise((resolve, reject) => { w.once('message', msg => msg.ready ? resolve() : reject(msg)); w.once('error', reject); })));
  const results = workers.map(w => new Promise((resolve, reject) => { w.once('message', msg => msg.error ? reject(new Error(msg.error)) : resolve(msg.result)); }));
  workers.forEach(w => w.postMessage('go'));
  const result = await Promise.all(results); await Promise.all(exits); return result;
}

test('explicit schema initialization: WAL/FULL/FK; open does not migrate; foreign database is untouched', t => {
  const f = fixture(t);
  assert.deepEqual(f.store.getDiagnostics(), { _schema_version: 2, _journal_mode: 'wal', _synchronous: 2, _foreign_keys: 1 });
  const missing = new SQLiteXNetStore({ ...f.options, _db_path: path.join(f.dir, 'missing.sqlite') });
  rejects(assert, () => missing.open(), 'E_XNET_NOT_READY');
  const foreignPath = path.join(f.dir, 'foreign.sqlite'); const foreign = new Database(foreignPath);
  foreign.exec('CREATE TABLE business(value TEXT); INSERT INTO business VALUES (\'keep\')'); foreign.close();
  const before = readFileSync(foreignPath);
  const other = new SQLiteXNetStore({ ...f.options, _db_path: foreignPath });
  rejects(assert, () => other.initialize(), 'E_XNET_SCHEMA_VERSION');
  assert.deepEqual(readFileSync(foreignPath), before);
  f.store.close(); const edit = new Database(f.options._db_path); edit.pragma('user_version=3'); edit.close();
  rejects(assert, () => f.store.open(), 'E_XNET_SCHEMA_VERSION');
});

test('two trusted receiving accounts route to isolated tenants and endpoints', t => {
  const f = fixture(t);
  const accepted = f.senders.map(p => f.store.accept(p, f.intent('same-provider-event')));
  assert.notEqual(accepted[0]._message_id, accepted[1]._message_id);
  for (let i = 0; i < 2; i++) {
    const d = f.dispatch(f.register(i), `receive-${i}`, i);
    assert.deepEqual(d._envelope._destination, f.addresses[i]);
    assert.equal(d._envelope._tenant_id, f.tenants[i]);
    assert.deepEqual(d._envelope._source, { ...f.sources[i], _principal_id: f.senders[i]._principal_id });
    assert.equal(f.store.acknowledge(f.receivers[i], f.ack(d))._state, 'processed');
  }
});

test('unauthorized, cross-tenant, wildcard, missing and payload-authority routes fail closed', t => {
  const f = fixture(t); const sender = f.senders[0];
  rejects(assert, () => f.store.accept(sender, f.intent('x', { destination: f.addresses[1] })), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.accept(sender, f.intent('x', { _tenant_id: f.tenants[1] })), 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => f.store.accept(sender, f.intent('x', { _source: f.sources[1] })), 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => f.store.accept(sender, f.intent('x', { message_type: 'unknown.v1' })), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.accept({ ...sender, _principal_id: randomUUID() }, f.intent()), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.accept({ ...sender, _tenant_id: f.tenants[1] }, f.intent()), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.registerEndpoint(f.receivers[0], f.addresses[1]), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.setRouteBinding({ ...f.routes[0], _version: 2, _destination: f.addresses[1] }, 1), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.setRouteBinding({ ...f.routes[0], _version: 2, _message_type: '*' }, 1), 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => f.store.setRouteBinding({ ...f.routes[0], _version: 2 }, 0), 'E_XNET_VERSION_CONFLICT');
  assert.equal(rows(f, 'xnet_message').length, 0);
});

test('duplicate acceptance returns original ID/destination even after route is disabled', t => {
  const f = fixture(t); const first = f.store.accept(f.senders[0], f.intent());
  f.store.setRouteBinding({ ...f.routes[0], _version: 2, _enabled: false }, 1);
  const duplicate = f.store.accept(f.senders[0], f.intent());
  assert.equal(duplicate._message_id, first._message_id); assert.equal(duplicate._duplicate, true);
  assert.deepEqual(f.store.getStatus(f.senders[0], first._message_id)._destination, f.addresses[0]);
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('event-2')), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('event-1', { payload: { ...f.intent().payload, _data: 'changed' } })), 'E_XNET_IDEMPOTENCY_CONFLICT');
  assert.equal(rows(f, 'xnet_delivery').length, 1);
});

test('endpoint-originated messages use an exact provisioned route; requested destinations cannot override it', t => {
  const f = fixture(t);
  const route = { ...f.routes[0], _id: randomUUID(), _source: { _kind: 'endpoint', _address: f.addresses[0] } };
  f.store.setRouteBinding(route, 0);
  const accepted = f.store.accept(f.receivers[0], f.intent('endpoint-send', { destination: f.addresses[0] }));
  const d = f.dispatch(f.register());
  assert.equal(d._envelope._message_id, accepted._message_id);
  assert.deepEqual(d._envelope._source, { _kind: 'endpoint', _address: f.addresses[0], _principal_id: f.receivers[0]._principal_id });
  rejects(assert, () => f.store.accept(f.receivers[0], f.intent('override', {
    destination: { _tenant_id: f.tenants[0], _endpoint_id: randomUUID() },
  })), 'E_XNET_FORBIDDEN');
});

test('concurrent separate-connection acceptance deduplicates atomically', async t => {
  const f = fixture(t);
  const accepted = await race(f, { principal: f.senders[0], intent: f.intent() });
  assert.equal(new Set(accepted.map(a => a._message_id)).size, 1);
  assert.equal(accepted.filter(a => !a._duplicate).length, 1);
  assert.equal(rows(f, 'xnet_message').length, 1); assert.equal(rows(f, 'xnet_delivery').length, 1);
});

test('concurrent separate-connection claims produce exactly one active owner and attempt', async t => {
  const f = fixture(t); f.store.accept(f.senders[0], f.intent()); const reg = f.register();
  const claims = await race(f, { principal: f.receivers[0], registration_id: reg._id });
  assert.equal(claims.filter(Boolean).length, 1); assert.equal(rows(f, 'xnet_attempt').length, 1);
  assert.equal(rows(f, 'xnet_delivery')[0].attempt_count, 1);
});

test('acceptance, mocked batch, claim and ACK failures roll back their entire transaction', t => {
  const f = fixture(t);
  for (const point of ['accept.after-message', 'accept.after-delivery']) {
    f.fault = at => { if (at === point) throw new Error('private disk failure detail'); };
    rejects(assert, () => f.store.accept(f.senders[0], f.intent()), 'E_XNET_STORE_UNAVAILABLE');
    assert.equal(rows(f, 'xnet_message').length, 0); assert.equal(rows(f, 'xnet_delivery').length, 0);
    assert.equal(rows(f, 'xnet_message_lifecycle').length, 0);
  }
  f.fault = undefined;
  rejects(assert, () => f.store.acceptBatch(f.senders[0], [f.intent(), f.intent('bad', { message_type: 'unknown.v1' })]), 'E_XNET_FORBIDDEN');
  assert.equal(rows(f, 'xnet_message').length, 0);
  assert.equal(rows(f, 'xnet_message_lifecycle').length, 0);
  const accepted = f.store.accept(f.senders[0], f.intent()); const reg = f.register();
  f.fault = at => { if (at === 'claim.after-attempt') throw new Error('claim failure'); };
  rejects(assert, () => f.claim(reg), 'E_XNET_STORE_UNAVAILABLE');
  assert.equal(rows(f, 'xnet_attempt').length, 0); assert.equal(f.store.getStatus(f.senders[0], accepted._message_id)._attempt_count, 0);
  f.fault = undefined; const d = f.dispatch(reg); const ack = f.ack(d);
  f.fault = at => { if (at === 'ack.after-receipt') throw new Error('ack failure'); };
  rejects(assert, () => f.store.acknowledge(f.receivers[0], ack), 'E_XNET_STORE_UNAVAILABLE');
  assert.equal(rows(f, 'xnet_ack').length, 0); assert.equal(f.store.getStatus(f.senders[0], accepted._message_id)._state, 'dispatched');
  assert.equal(rows(f, 'xnet_message_lifecycle')[0].terminal_at, null);
  f.fault = undefined; assert.equal(f.store.acknowledge(f.receivers[0], ack)._state, 'processed');
  assert.equal(rows(f, 'xnet_message_lifecycle')[0].terminal_at, f.now);
});

test('process crash inside acceptance leaves no partial handoff; restart retry succeeds', t => {
  const f = fixture(t);
  const code = `import {SQLiteXNetStore} from './dist/server/index.js';
    const s=new SQLiteXNetStore({_db_path:process.env.XNET_TEST_DB,_claim_key:Buffer.alloc(32,42),_fault:()=>process.exit(17)});
    s.open();s.accept(JSON.parse(process.env.XNET_TEST_P),JSON.parse(process.env.XNET_TEST_INTENT));`;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', code], {
    cwd: path.resolve(import.meta.dirname, '..'), env: { ...process.env,
      XNET_TEST_DB: f.options._db_path, XNET_TEST_P: JSON.stringify(f.senders[0]), XNET_TEST_INTENT: JSON.stringify(f.intent()) }, encoding: 'utf8',
  });
  assert.equal(child.status, 17, child.stderr);
  assert.equal(rows(f, 'xnet_message').length, 0); assert.equal(rows(f, 'xnet_delivery').length, 0);
  assert.equal(f.store.accept(f.senders[0], f.intent())._duplicate, false);
});

test('same receive request replays its token; stale/expired/mismatched ACKs do not mutate state', t => {
  const f = fixture(t); const a = f.store.accept(f.senders[0], f.intent()); const reg = f.register();
  const d = f.dispatch(reg, 'lost-response');
  assert.deepEqual(f.claim(reg, 'lost-response'), d);
  assert.equal(f.claim(reg, 'different-request'), null);
  const ack = f.ack(d);
  for (const bad of [{ owner_epoch: ack.owner_epoch + 1 }, { attempt_id: randomUUID() }, { claim_token: 'bad-token' }, { message_id: randomUUID() }]) {
    rejects(assert, () => f.store.acknowledge(f.receivers[0], { ...ack, ...bad }), 'E_XNET_STALE_CLAIM');
  }
  f.now += 50;
  rejects(assert, () => f.store.acknowledge(f.receivers[0], ack), 'E_XNET_STALE_CLAIM');
  assert.equal(rows(f, 'xnet_ack').length, 0);
  f.store.advance(); assert.equal(f.store.getStatus(f.senders[0], a._message_id)._state, 'retry_wait');
  f.now += 10; f.store.advance(); const next = f.dispatch(reg);
  assert.notEqual(next._attempt_id, d._attempt_id);
  rejects(assert, () => f.store.acknowledge(f.receivers[0], ack), 'E_XNET_STALE_CLAIM');
  rejects(assert, () => f.claim(reg, 'lost-response'), 'E_XNET_STALE_CLAIM');
});

test('processed ACK is durable and idempotent, including retry after processing lease expiry', t => {
  const f = fixture(t); f.store.accept(f.senders[0], f.intent()); const d = f.dispatch(f.register()); const ack = f.ack(d);
  const processed = f.store.acknowledge(f.receivers[0], ack); assert.equal(processed._state, 'processed');
  f.now += 51;
  assert.deepEqual(f.store.acknowledge(f.receivers[0], ack), processed);
  rejects(assert, () => f.store.acknowledge(f.receivers[0], { ...ack, receipt_id: randomUUID() }), 'E_XNET_IDEMPOTENCY_CONFLICT');
  assert.equal(rows(f, 'xnet_ack').length, 1);
  assert.equal(JSON.stringify(rows(f, 'xnet_attempt')).includes(d._claim_token), false);
  assert.equal(JSON.stringify(processed).includes('simulated message'), false);
});

test('offline messages stay queued without consuming attempts; capacity is checked before acceptance', t => {
  const f = fixture(t, { _max_queued_per_tenant: 1, _max_queued_server: 2 });
  const a = f.store.accept(f.senders[0], f.intent()); f.now += 500; f.store.advance();
  const status = f.store.getStatus(f.senders[0], a._message_id);
  assert.equal(status._state, 'queued'); assert.equal(status._attempt_count, 0);
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('new')), 'E_XNET_CAPACITY');
  assert.equal(f.store.accept(f.senders[0], f.intent())._duplicate, true);
  f.store.accept(f.senders[1], f.intent()); assert.equal(rows(f, 'xnet_message').length, 2);
  const d = f.dispatch(f.register()); assert.equal(d._envelope._message_id, a._message_id);
});

test('bounded retries, explicit uncertain outcomes, permanent failures and TTL expiry', t => {
  const f = fixture(t); const a = f.store.accept(f.senders[0], f.intent()); const reg = f.register();
  for (let n = 1; n <= 3; n++) {
    const d = f.dispatch(reg); assert.ok(d);
    const status = f.store.acknowledge(f.receivers[0], f.ack(d, { outcome: 'retryable_failure', receipt_id: undefined }));
    assert.equal(status._attempt_count, n); assert.equal(status._state, n === 3 ? 'dead_letter' : 'retry_wait');
    f.now = status._next_attempt_at; f.store.advance();
  }
  assert.equal(f.claim(reg), null); assert.equal(f.store.getStatus(f.senders[0], a._message_id)._state, 'dead_letter');
  f.store.accept(f.senders[0], f.intent('uncertain'));
  const uncertain = f.dispatch(reg);
  f.store.acknowledge(f.receivers[0], f.ack(uncertain, { outcome: 'uncertain', receipt_id: undefined, error_code: 'TARGET_UNCERTAIN' }));
  f.store.accept(f.senders[0], f.intent('permanent'));
  const permanent = f.dispatch(reg);
  assert.equal(f.store.acknowledge(f.receivers[0], f.ack(permanent, {
    outcome: 'permanent_failure', receipt_id: undefined, error_code: 'TARGET_PERMANENT',
  }))._state, 'dead_letter');
  const expired = f.store.accept(f.senders[0], f.intent('expires', { ttl_ms: 5 }));
  f.now += 5000; f.store.advance();
  assert.equal(f.store.getStatus(f.senders[0], uncertain._envelope._message_id)._state, 'uncertain');
  assert.equal(f.store.getStatus(f.senders[0], expired._message_id)._state, 'expired');
});

test('lease exhaustion and expiry during a dispatch are bounded terminal transitions', t => {
  const f = fixture(t, { _max_attempts: 2 }); const a = f.store.accept(f.senders[0], f.intent()); const reg = f.register();
  for (let i = 0; i < 2; i++) {
    const d = f.dispatch(reg); f.now = d._claim_expires_at; f.store.advance();
    const s = f.store.getStatus(f.senders[0], a._message_id);
    assert.equal(s._state, i === 1 ? 'dead_letter' : 'retry_wait');
    f.now = s._next_attempt_at; f.store.advance();
  }
  assert.equal(f.claim(reg), null);
  f.store.accept(f.senders[0], f.intent('expires-in-flight', { ttl_ms: 2 })); const d = f.dispatch(reg);
  f.now += 2; f.store.advance();
  assert.equal(f.store.getStatus(f.senders[0], d._envelope._message_id)._state, 'expired');
  rejects(assert, () => f.store.acknowledge(f.receivers[0], f.ack(d)), 'E_XNET_STALE_CLAIM');
});

test('restart persists accepted/dedup data and fences prior registration; backup restores consistently', async t => {
  const f = fixture(t); const accepted = f.store.accept(f.senders[0], f.intent()); const reg = f.register(); const old = f.dispatch(reg);
  f.store.close();
  f.store = new SQLiteXNetStore({ ...f.options, _instance_id: randomUUID() }); f.store.open(); f.stores.push(f.store);
  assert.equal(f.store.accept(f.senders[0], f.intent())._message_id, accepted._message_id);
  rejects(assert, () => f.store.acknowledge(f.receivers[0], f.ack(old)), 'E_XNET_STALE_CLAIM');
  const nextReg = f.register(); assert.ok(nextReg._owner_epoch > reg._owner_epoch);
  f.now += 50; f.store.advance(); f.now += 10; f.store.advance();
  const d = f.dispatch(nextReg); assert.equal(d._envelope._message_id, accepted._message_id);
  f.store.acknowledge(f.receivers[0], f.ack(d));
  const snapshot = path.join(f.dir, 'snapshot.sqlite'); await f.store.backup(snapshot);
  const restored = new SQLiteXNetStore({ ...f.options, _db_path: snapshot }); restored.open(); f.stores.push(restored);
  assert.equal(restored.getStatus(f.senders[0], accepted._message_id)._state, 'processed');
  assert.equal(restored.accept(f.senders[0], f.intent())._duplicate, true);
});

test('consumer-owned receipt/effect transaction prevents duplicate processing after lost ACK', t => {
  const f = fixture(t); f.store.accept(f.senders[0], f.intent()); const reg = f.register();
  const target = new Database(path.join(f.dir, 'target.sqlite')); t.after(() => target.close());
  target.pragma('journal_mode=WAL'); target.pragma('synchronous=FULL');
  target.exec('CREATE TABLE receipt(tenant TEXT, endpoint TEXT, message TEXT, id TEXT, PRIMARY KEY(tenant,endpoint,message)); CREATE TABLE effect(id TEXT PRIMARY KEY)');
  const processTarget = target.transaction(d => {
    const e = d._envelope;
    const old = target.prepare('SELECT id FROM receipt WHERE tenant=? AND endpoint=? AND message=?').get(e._tenant_id, e._destination._endpoint_id, e._message_id);
    if (old) return old.id;
    const receipt = randomUUID(); target.prepare('INSERT INTO effect VALUES (?)').run(e._message_id);
    target.prepare('INSERT INTO receipt VALUES (?,?,?,?)').run(e._tenant_id, e._destination._endpoint_id, e._message_id, receipt); return receipt;
  });
  const first = f.dispatch(reg); const receipt = processTarget(first); // ACK deliberately lost.
  f.now += 50; f.store.advance(); f.now += 10; f.store.advance(); const second = f.dispatch(reg);
  assert.equal(processTarget(second), receipt);
  assert.equal(target.prepare('SELECT count(*) AS n FROM effect').get().n, 1);
  assert.equal(f.store.acknowledge(f.receivers[0], f.ack(second, { receipt_id: receipt }))._state, 'processed');
});

test('cross-tenant status, claims, causation and receipt reuse are rejected without data disclosure', t => {
  const f = fixture(t); const a = f.store.accept(f.senders[0], f.intent()); const reg = f.register(); const d = f.dispatch(reg);
  rejects(assert, () => f.store.getStatus(f.senders[1], a._message_id), 'E_XNET_FORBIDDEN');
  rejects(assert, () => f.store.claimNext(f.receivers[1], reg._id, 'steal'), 'E_XNET_STALE_CLAIM');
  rejects(assert, () => f.store.acknowledge(f.receivers[1], f.ack(d)), 'E_XNET_STALE_CLAIM');
  rejects(assert, () => f.store.accept(f.senders[1], f.intent('cross-parent', { causation_id: a._message_id })), 'E_XNET_FORBIDDEN');
  const receipt_id = randomUUID(); f.store.acknowledge(f.receivers[0], f.ack(d, { receipt_id }));
  f.store.accept(f.senders[0], f.intent('next')); const next = f.dispatch(reg);
  rejects(assert, () => f.store.acknowledge(f.receivers[0], f.ack(next, { receipt_id })), 'E_XNET_IDEMPOTENCY_CONFLICT');
  assert.equal(rows(f, 'xnet_ack').length, 1);
});
