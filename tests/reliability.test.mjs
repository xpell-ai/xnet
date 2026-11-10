import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash, randomUUID } from 'node:crypto';
import { existsSync, readdirSync } from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { XpellEngine } from '@xpell/core';
import { assertXNetContract, assertXNetDispatch, XNET_LIMITS, XNET_DISPATCH_LIMITS } from '@xpell/xnet/shared';
import { createXNetModule, SQLiteXNetStore } from '@xpell/xnet/server';
import { fixture, TYPE, rejects } from './fixtures.mjs';

function rows(f, table) {
  const db = new Database(f.options._db_path, { readonly: true });
  try { return db.prepare(`SELECT * FROM ${table} ORDER BY 1,2`).all(); } finally { db.close(); }
}
function reopen(f, limits = {}) {
  f.store.close();
  f.store = new SQLiteXNetStore({ ...f.options, _limits: { ...f.options._limits, ...limits } });
  f.store.open(); f.stores.push(f.store);
}
async function commands(t, f) {
  // Core deliberately has no module replacement/unload; use an isolated public engine.
  const _x = new XpellEngine();
  const authority = new WeakMap();
  const module = createXNetModule({ _store: f.store, _authorize: cmd => authority.get(cmd) });
  await _x.loadModuleAsync(module); module.start(); t.after(() => module.stop());
  return (p, op, params) => {
    const cmd = { _module: 'xnet', _op: op, _params: params };
    authority.set(cmd, p); return _x.execute(cmd);
  };
}
const payload = (f, data) => ({ ...f.intent().payload, _data: data });

test('accepted 64 KiB envelopes fit dispatch wrappers; one byte over fails before acceptance', async t => {
  const f = fixture(t); const call = await commands(t, f);
  f.store.accept(f.senders[1], f.intent('boundary-0', { payload: payload(f, '') }));
  const overhead = rows(f, 'xnet_message')[0].envelope_bytes;
  const data = 'x'.repeat(XNET_LIMITS._max_envelope_bytes - overhead);
  const accepted = await call(f.senders[0], 'submit', f.intent('boundary-1', { payload: payload(f, data) }));
  assert.equal(accepted._ok, true);
  const reg = await call(f.receivers[0], 'register-endpoint', { address: f.addresses[0] });
  const received = await call(f.receivers[0], 'receive-next', { registration_id: reg._result._id, request_key: 'boundary' });
  assert.equal(received._ok, true); assertXNetDispatch(received._result);
  assert.equal(Buffer.byteLength(JSON.stringify(received._result._envelope)), 65536);
  assert.ok(Buffer.byteLength(JSON.stringify(received._result)) > 65536);
  // Worst permitted wrapper scalars still fit the published budget.
  const worst = { ...received._result, _claim_token: '\u0000'.repeat(128),
    _owner_epoch: Number.MAX_SAFE_INTEGER, _claim_expires_at: Number.MAX_SAFE_INTEGER };
  assertXNetDispatch(worst); assertXNetContract('dispatch', worst);
  assert.ok(Buffer.byteLength(JSON.stringify(worst)) <= XNET_DISPATCH_LIMITS._max_bytes);
  rejects(assert, () => assertXNetContract('dispatch', { ...worst, _envelope: { ...worst._envelope,
    _payload: payload(f, data + 'x') } }), 'E_XNET_TOO_LARGE');
  assert.equal((await call(f.receivers[0], 'ack-processed', f.ack(received._result)))._result._state, 'processed');
  const before = rows(f, 'xnet_message');
  const rejected = await call(f.senders[0], 'submit', f.intent('boundary-2', { payload: payload(f, data + 'x') }));
  assert.equal(rejected._result._code, 'E_XNET_TOO_LARGE');
  assert.deepEqual(rows(f, 'xnet_message'), before);
});

test('maximum envelope depth survives canonical submit/receive/ACK; deeper inputs are rejected', async t => {
  const f = fixture(t); const call = await commands(t, f);
  let data = 'leaf'; for (let i = 0; i < 14; i++) data = { nested: data };
  const accepted = await call(f.senders[0], 'submit', f.intent('deep', { payload: payload(f, data) }));
  assert.equal(accepted._ok, true);
  const reg = f.register();
  const result = await call(f.receivers[0], 'receive-next', { registration_id: reg._id, request_key: 'deep' });
  assert.equal(result._ok, true); assertXNetDispatch(result._result);
  assert.equal((await call(f.receivers[0], 'ack-processed', f.ack(result._result)))._result._state, 'processed');
  const before = rows(f, 'xnet_message');
  const bad = await call(f.senders[0], 'submit', f.intent('too-deep', { payload: payload(f, { nested: data }) }));
  assert.equal(bad._result._code, 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('direct-too-deep', { payload: payload(f, { nested: data }) })), 'E_XNET_INVALID_MESSAGE');
  assert.deepEqual(rows(f, 'xnet_message'), before);
});

test('custom envelope byte ceilings are enforced before any durable acceptance', t => {
  const f = fixture(t, { _max_envelope_bytes: 1200 });
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('over', { payload: payload(f, 'x'.repeat(1200)) })), 'E_XNET_TOO_LARGE');
  assert.equal(rows(f, 'xnet_message').length, 0); assert.equal(rows(f, 'xnet_delivery').length, 0);
  assert.equal(rows(f, 'xnet_message_lifecycle').length, 0);
});

test('near-expiry claims advertise ownership deadlines and remain ACKable until that deadline', t => {
  const f = fixture(t, { _ttl_ms: 200_000, _claim_lease_ms: 60_000, _registration_lease_ms: 60_000 });
  f.store.accept(f.senders[0], f.intent()); const reg = f.register(); f.now += 59_000;
  const d = f.dispatch(reg, 'near-expiry');
  assert.equal(d._claim_expires_at, reg._lease_expires_at);
  assert.deepEqual(f.claim(reg, 'near-expiry'), d);
  assert.deepEqual(f.register(), reg); // No implicit renewal or epoch change.
  f.now = d._claim_expires_at - 1;
  assert.equal(f.store.acknowledge(f.receivers[0], f.ack(d))._state, 'processed');
});

test('competing registration, reconnect and expired claims preserve fencing', t => {
  const f = fixture(t, { _registration_lease_ms: 20, _claim_lease_ms: 50 });
  const competitor = { _principal_id: randomUUID(), _tenant_id: f.tenants[0] };
  f.store.provision({ tenants: [], accounts: [], endpoints: [], routes: [], grants: [{ ...competitor,
    _enabled: true, _permissions: ['receive', 'ack'], _receive_endpoint_ids: [f.addresses[0]._endpoint_id] }] });
  const a = f.store.accept(f.senders[0], f.intent()); const reg = f.register(); const d = f.dispatch(reg);
  rejects(assert, () => f.store.registerEndpoint(competitor, f.addresses[0]), 'E_XNET_OWNER_CONFLICT');
  assert.equal(d._claim_expires_at, reg._lease_expires_at);
  f.now = d._claim_expires_at;
  rejects(assert, () => f.store.acknowledge(f.receivers[0], f.ack(d)), 'E_XNET_STALE_CLAIM');
  const replacement = f.register(); assert.ok(replacement._owner_epoch > reg._owner_epoch);
  rejects(assert, () => f.claim(reg), 'E_XNET_STALE_CLAIM');
  rejects(assert, () => f.store.markDispatched(f.receivers[0], d), 'E_XNET_STALE_CLAIM');
  f.store.advance(); f.now += 10; f.store.advance();
  const next = f.dispatch(replacement); assert.notEqual(next._attempt_id, d._attempt_id);
  rejects(assert, () => f.store.acknowledge(f.receivers[0], f.ack(d)), 'E_XNET_STALE_CLAIM');
  assert.equal(f.store.acknowledge(f.receivers[0], f.ack(next))._state, 'processed');
  assert.equal(f.store.getStatus(f.senders[0], a._message_id)._attempt_count, 2);
});

test('historical claim replay clips advertised validity without rewriting persisted history', t => {
  const f = fixture(t, { _registration_lease_ms: 20, _claim_lease_ms: 50 });
  f.store.accept(f.senders[0], f.intent()); const reg = f.register(); const d = f.claim(reg, 'historical');
  // Arrange a pre-correction lease on disposable storage, retaining its original token/fence.
  const db = new Database(f.options._db_path);
  db.prepare('UPDATE xnet_attempt SET expires_at=? WHERE id=?').run(f.now + 50, d._attempt_id);
  db.prepare('UPDATE xnet_delivery SET claim_expires_at=? WHERE id=?').run(f.now + 50, d._delivery_id); db.close();
  const attempts = rows(f, 'xnet_attempt'); const deliveries = rows(f, 'xnet_delivery');
  const replayed = f.claim(reg, 'historical'); assert.equal(replayed._claim_expires_at, reg._lease_expires_at);
  assert.deepEqual(rows(f, 'xnet_attempt'), attempts); assert.deepEqual(rows(f, 'xnet_delivery'), deliveries);
  f.store.markDispatched(f.receivers[0], replayed); f.now = replayed._claim_expires_at - 1;
  assert.equal(f.store.acknowledge(f.receivers[0], f.ack(replayed))._state, 'processed');
});

test('backup refuses a destination created after precheck and cleans private artifacts', async t => {
  const f = fixture(t); f.store.accept(f.senders[0], f.intent()); const destination = path.join(f.dir, 'race.sqlite');
  const pending = f.store.backup(destination);
  const unrelated = new Database(destination);
  unrelated.exec("CREATE TABLE keep_me(value TEXT); INSERT INTO keep_me VALUES ('sentinel')"); unrelated.close();
  await assert.rejects(pending, e => e._code === 'E_XNET_INVALID_CONFIG');
  const db = new Database(destination); t.after(() => db.close());
  assert.equal(db.prepare('SELECT value FROM keep_me').get().value, 'sentinel');
  assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='xnet_message'").get().n, 0);
  assert.deepEqual(readdirSync(f.dir).filter(p => p.startsWith('.xnet-backup-')), []);
  await assert.rejects(f.store.backup(destination), e => e._code === 'E_XNET_INVALID_CONFIG');
});

test('competing backups publish once and the winning snapshot restores consistently', async t => {
  const f = fixture(t); const a = f.store.accept(f.senders[0], f.intent()); const d = f.dispatch(f.register());
  f.store.acknowledge(f.receivers[0], f.ack(d));
  const destination = path.join(f.dir, 'winner.sqlite');
  const results = await Promise.allSettled([f.store.backup(destination), f.store.backup(destination)]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(results.find(r => r.status === 'rejected').reason._code, 'E_XNET_INVALID_CONFIG');
  const restored = new SQLiteXNetStore({ ...f.options, _db_path: destination }); restored.open(); f.stores.push(restored);
  assert.equal(restored.getStatus(f.senders[0], a._message_id)._state, 'processed');
  assert.equal(restored.accept(f.senders[0], f.intent())._message_id, a._message_id);
  assert.deepEqual(readdirSync(f.dir).filter(p => p.startsWith('.xnet-backup-')), []);
});

test('failed SQLite backup removes private temporary files without publishing a destination', async t => {
  const f = fixture(t); const destination = path.join(f.dir, 'failure.sqlite');
  const pending = f.store.backup(destination); f.store.close();
  await assert.rejects(pending, e => e._code === 'E_XNET_STORE_UNAVAILABLE');
  assert.equal(existsSync(destination), false);
  assert.deepEqual(readdirSync(f.dir).filter(p => p.startsWith('.xnet-backup-')), []);
});

test('duplicate replay survives restart/default changes and retains historical identity and destination', t => {
  const f = fixture(t); const intent = f.intent(); const first = f.store.accept(f.senders[0], intent);
  const messages = rows(f, 'xnet_message'); const deliveries = rows(f, 'xnet_delivery');
  reopen(f, { _ttl_ms: 500 });
  f.store.setRouteBinding({ ...f.routes[0], _version: 2, _enabled: false }, 1);
  const duplicate = f.store.accept(f.senders[0], intent);
  assert.deepEqual(duplicate, { ...first, _duplicate: true });
  assert.deepEqual(f.store.getStatus(f.senders[0], first._message_id)._destination, f.addresses[0]);
  for (const extra of [{ payload: payload(f, 'different') }, { ttl_ms: 1000 }, { idempotency_key: 'another-intent' }]) {
    rejects(assert, () => f.store.accept(f.senders[0], { ...intent, ...extra }), 'E_XNET_IDEMPOTENCY_CONFLICT');
  }
  assert.deepEqual(rows(f, 'xnet_message'), messages); assert.deepEqual(rows(f, 'xnet_delivery'), deliveries);
  rejects(assert, () => f.store.getStatus(f.senders[1], first._message_id), 'E_XNET_FORBIDDEN');
});

test('explicit and omitted TTL share configured bounds without clamping', t => {
  const f = fixture(t, { _ttl_ms: 172_800_000 });
  for (const [id, extra] of [['omitted', {}], ['explicit', { ttl_ms: 172_800_000 }], ['short', { ttl_ms: 5 }]]) {
    const a = f.store.accept(f.senders[0], f.intent(id, extra));
    assert.equal(a._expires_at - a._accepted_at, extra.ttl_ms ?? 172_800_000);
  }
  for (const ttl_ms of [172_800_001, 0, -1, 1.5]) {
    rejects(assert, () => f.store.accept(f.senders[0], f.intent('invalid', { ttl_ms })), 'E_XNET_INVALID_MESSAGE');
  }
  const explicit = f.intent('explicit'); explicit.ttl_ms = 172_800_000;
  reopen(f, { _ttl_ms: 500 });
  assert.equal(f.store.accept(f.senders[0], explicit)._duplicate, true); // Existing acceptance, no new TTL granted.
  rejects(assert, () => f.store.accept(f.senders[0], { ...explicit, event_id: 'new', idempotency_key: 'new' }), 'E_XNET_INVALID_MESSAGE');
  const omitted = f.store.accept(f.senders[0], f.intent('new-omitted'));
  assert.equal(omitted._expires_at - omitted._accepted_at, 500);
});

test('terminal cleanup is bounded, transactional and preserves active/uncertain work and receipt evidence', t => {
  const f = fixture(t, { _terminal_payload_retention_ms: 100, _advance_limit: 2, _ttl_ms: 10_000, _claim_lease_ms: 500 });
  const regs = [f.register(0), f.register(1)]; const terminal = [];
  for (let i = 0; i < 2; i++) {
    const a = f.store.accept(f.senders[i], f.intent('processed')); const d = f.dispatch(regs[i], randomUUID(), i);
    f.store.acknowledge(f.receivers[i], f.ack(d)); terminal.push(a._message_id);
  }
  const dead = f.store.accept(f.senders[0], f.intent('dead')); terminal.push(dead._message_id);
  f.store.acknowledge(f.receivers[0], f.ack(f.dispatch(regs[0]), { outcome: 'permanent_failure', receipt_id: undefined }));
  const expired = f.store.accept(f.senders[0], f.intent('expired', { ttl_ms: 1 })); terminal.push(expired._message_id);
  f.now++; f.store.advance();
  f.store.accept(f.senders[0], f.intent('uncertain'));
  f.store.acknowledge(f.receivers[0], f.ack(f.dispatch(regs[0]), { outcome: 'uncertain', receipt_id: undefined }));
  f.store.accept(f.senders[0], f.intent('retry'));
  f.store.acknowledge(f.receivers[0], f.ack(f.dispatch(regs[0]), { outcome: 'retryable_failure', receipt_id: undefined }));
  f.store.accept(f.senders[0], f.intent('leased')); f.claim(regs[0]);
  f.store.accept(f.senders[0], f.intent('queued'));
  f.store.accept(f.senders[1], f.intent('dispatched')); f.dispatch(regs[1], randomUUID(), 1);
  const before = rows(f, 'xnet_message'); const deliveries = rows(f, 'xnet_delivery');
  const attempts = rows(f, 'xnet_attempt'); const acks = rows(f, 'xnet_ack');
  f.now = 1_000_099; assert.equal(f.store.cleanupTerminalPayloads(), 0);
  f.now = 1_000_101;
  rejects(assert, () => f.store.cleanupTerminalPayloads(3), 'E_XNET_INVALID_MESSAGE');
  assert.equal(f.store.cleanupTerminalPayloads(), 2);
  assert.equal(rows(f, 'xnet_message').filter(m => !Object.hasOwn(JSON.parse(m.envelope), '_payload')).length, 2);
  assert.equal(f.store.cleanupTerminalPayloads(), 2); assert.equal(f.store.cleanupTerminalPayloads(), 0);
  for (const m of rows(f, 'xnet_message')) {
    const old = before.find(p => p.id === m.id);
    if (terminal.includes(m.id)) {
      assert.deepEqual(m, { ...old, envelope: m.envelope, envelope_bytes: Buffer.byteLength(m.envelope) });
      const header = JSON.parse(m.envelope); assert.equal(Object.hasOwn(header, '_payload'), false);
      for (const [key, value] of Object.entries(JSON.parse(old.envelope))) {
        if (key !== '_payload') assert.deepEqual(header[key], value);
      }
    } else assert.deepEqual(m, old);
  }
  assert.deepEqual(rows(f, 'xnet_delivery'), deliveries); assert.deepEqual(rows(f, 'xnet_attempt'), attempts);
  assert.deepEqual(rows(f, 'xnet_ack'), acks);
  assert.equal(f.store.accept(f.senders[0], f.intent('processed'))._duplicate, true);
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('processed', { payload: payload(f, 'changed') })), 'E_XNET_IDEMPOTENCY_CONFLICT');
  rejects(assert, () => f.store.getStatus(f.senders[1], terminal[0]), 'E_XNET_FORBIDDEN');
  f.now += 31 * 86_400_000; assert.equal(f.store.cleanupTerminalPayloads(), 0);
  assert.deepEqual(rows(f, 'xnet_ack'), acks); // Evidence outlives the minimum replay horizon.
  assert.equal(rows(f, 'xnet_message').length, before.length);
});

test('cleanup failure rolls back payload and marker; restart preserves tombstones and safe status', t => {
  const f = fixture(t, { _terminal_payload_retention_ms: 10 });
  const a = f.store.accept(f.senders[0], f.intent()); const d = f.dispatch(f.register());
  const status = f.store.acknowledge(f.receivers[0], f.ack(d)); f.now += 10;
  const messages = rows(f, 'xnet_message'); const lifecycle = rows(f, 'xnet_message_lifecycle');
  f.fault = point => { if (point === 'cleanup.after-payload') throw new Error('simulated maintenance failure'); };
  rejects(assert, () => f.store.cleanupTerminalPayloads(), 'E_XNET_STORE_UNAVAILABLE');
  assert.deepEqual(rows(f, 'xnet_message'), messages); assert.deepEqual(rows(f, 'xnet_message_lifecycle'), lifecycle);
  f.fault = undefined; assert.equal(f.store.cleanupTerminalPayloads(), 1); reopen(f);
  assert.deepEqual(f.store.getStatus(f.senders[0], a._message_id), status);
  assert.equal(f.store.accept(f.senders[0], f.intent())._message_id, a._message_id);
  assert.deepEqual(f.store.acknowledge(f.receivers[0], f.ack(d, { receipt_id: status._receipt_id })), status);
});

test('default retention ends at seven days and unsafe retention settings are rejected', t => {
  const f = fixture(t); f.store.accept(f.senders[0], f.intent()); const d = f.dispatch(f.register());
  f.store.acknowledge(f.receivers[0], f.ack(d)); const start = f.now;
  f.now = start + 7 * 86_400_000 - 1; assert.equal(f.store.cleanupTerminalPayloads(), 0);
  f.now++; assert.equal(f.store.cleanupTerminalPayloads(), 1);
  for (const value of [0, -1, 7 * 86_400_000 + 1]) {
    rejects(assert, () => new SQLiteXNetStore({ ...f.options, _limits: { _terminal_payload_retention_ms: value } }), 'E_XNET_INVALID_CONFIG');
  }
});

test('explicit V1 upgrade leaves historical rows intact and preserves legacy duplicate comparison', t => {
  const f = fixture(t, { _terminal_payload_retention_ms: 100 });
  const a = f.store.accept(f.senders[0], f.intent()); const d = f.dispatch(f.register());
  f.store.acknowledge(f.receivers[0], f.ack(d));
  f.store.accept(f.senders[1], f.intent()); f.store.close();
  const db = new Database(f.options._db_path);
  // Reconstruct V1 on a disposable fixture: same ten-table schema and frozen V1 hash format.
  const legacySemantic = JSON.stringify({ causation_id: null, destination: null, message_type: TYPE,
    payload: { _content_type: 'application/json', _data: { text: 'simulated message' }, _mode: 'inline', _schema_id: 'mock.text.v1' }, ttl_ms: 1000 });
  db.prepare('UPDATE xnet_message SET content_hash=?').run(createHash('sha256').update(legacySemantic).digest('hex'));
  db.exec('DROP TABLE xnet_message_lifecycle'); db.pragma('user_version=1'); db.close();
  const tables = ['xnet_message', 'xnet_delivery', 'xnet_attempt', 'xnet_ack', 'xnet_registration'];
  const history = Object.fromEntries(tables.map(table => [table, rows(f, table)]));
  const upgraded = new SQLiteXNetStore({ ...f.options, _limits: { ...f.options._limits, _ttl_ms: 500 } });
  rejects(assert, () => upgraded.open(), 'E_XNET_SCHEMA_VERSION');
  upgraded.initialize(); f.store = upgraded; f.stores.push(upgraded);
  for (const table of tables) assert.deepEqual(rows(f, table), history[table]);
  assert.equal(upgraded.getDiagnostics()._schema_version, 2);
  assert.equal(upgraded.accept(f.senders[0], f.intent())._message_id, a._message_id);
  rejects(assert, () => upgraded.accept(f.senders[0], f.intent('event-1', { payload: payload(f, 'changed') })), 'E_XNET_IDEMPOTENCY_CONFLICT');
  assert.equal(upgraded.cleanupTerminalPayloads(), 1); // Unknown legacy terminal time starts a conservative grace window.
  assert.deepEqual(rows(f, 'xnet_message'), history.xnet_message);
  f.now += 100; assert.equal(upgraded.cleanupTerminalPayloads(), 1);
  assert.equal(upgraded.accept(f.senders[0], f.intent())._duplicate, true);
  assert.deepEqual(rows(f, 'xnet_delivery'), history.xnet_delivery); assert.deepEqual(rows(f, 'xnet_ack'), history.xnet_ack);
});

test('failed additive schema migration rolls back DDL and leaves V1 history unchanged', t => {
  const f = fixture(t); f.store.accept(f.senders[0], f.intent()); f.store.close();
  let db = new Database(f.options._db_path);
  db.exec('DROP TABLE xnet_message_lifecycle; CREATE INDEX xnet_payload_retention ON xnet_message(expires_at)');
  db.pragma('user_version=1'); db.close();
  const messages = rows(f, 'xnet_message'); const deliveries = rows(f, 'xnet_delivery');
  const migrating = new SQLiteXNetStore(f.options);
  rejects(assert, () => migrating.initialize(), 'E_XNET_STORE_UNAVAILABLE');
  assert.equal(migrating.isOpen(), false);
  db = new Database(f.options._db_path);
  assert.equal(db.pragma('user_version', { simple: true }), 1);
  assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='xnet_message_lifecycle'").get().n, 0);
  db.close();
  assert.deepEqual(rows(f, 'xnet_message'), messages); assert.deepEqual(rows(f, 'xnet_delivery'), deliveries);
});
