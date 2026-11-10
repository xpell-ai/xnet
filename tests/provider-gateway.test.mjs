import assert from 'node:assert/strict';
import test from 'node:test';
import { fork } from 'node:child_process';
import { createHmac, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { SQLiteXNetStore } from '@xpell/xnet/server';
import { validateBindings, TYPE } from './provider-fixtures/simulated-whatsapp.mjs';

const WABA = '200000000000001';
const SECRET = 'synthetic-signing-secret-never-a-real-meta-key';
const NOW = 1_000_000;
const req = (op, params = {}, extra = {}) => ({ _module: 'xnet', _op: op, _params: params, ...extra });
const rejected = promise => assert.rejects(promise);
function provision() {
  const tenants = [randomUUID(), randomUUID()];
  const addresses = tenants.map(_tenant_id => ({ _tenant_id, _endpoint_id: randomUUID() }));
  const endpoints = addresses.map(_address => ({ _id: _address._endpoint_id, _address, _kind: 'service',
    _name: 'simulated-target', _enabled: true, _accepted_message_types: [TYPE], _processing_contract: 'idempotent-v1' }));
  const accounts = tenants.map((_tenant_id, i) => ({ _id: randomUUID(), _tenant_id, _adapter_id: 'sim-whatsapp',
    _provider: 'whatsapp', _account_locator: `10000000000000${i + 1}`, _enabled: true }));
  const senders = tenants.map(_tenant_id => ({ _tenant_id, _principal_id: randomUUID() }));
  const receivers = tenants.map(_tenant_id => ({ _tenant_id, _principal_id: randomUUID() }));
  const sources = accounts.map(a => ({ _kind: 'provider_account', _adapter_id: a._adapter_id, _account_binding_id: a._id }));
  const routes = tenants.map((_tenant_id, i) => ({ _id: randomUUID(), _tenant_id, _source: sources[i], _version: 1,
    _destination: addresses[i], _message_type: TYPE, _enabled: true, _updated_at: NOW, _updated_by: randomUUID() }));
  const grants = tenants.flatMap((_tenant_id, i) => [
    { ...senders[i], _enabled: true, _permissions: ['submit', 'status'], _source: sources[i], _receive_endpoint_ids: [] },
    { ...receivers[i], _enabled: true, _permissions: ['receive', 'ack', 'status'], _receive_endpoint_ids: [addresses[i]._endpoint_id] },
  ]);
  const credentials = [...senders.map((p, i) => ({ _id: `producer-${i}`, _principal: p, _token: `synthetic-producer-${i}-${randomUUID()}` })),
    ...receivers.map((p, i) => ({ _id: `endpoint-${i}`, _principal: p, _token: `synthetic-endpoint-${i}-${randomUUID()}` }))];
  const gateway = { _profile: { _id: 'simulated-app', _secret: SECRET },
    _bindings: accounts.map((a, i) => ({ _profile_id: 'simulated-app', _receiving_phone_id: a._account_locator,
      _waba_id: WABA, _account_binding_id: a._id, _principal_id: senders[i]._principal_id, _credential_ref: `producer-${i}` })) };
  return { tenants, addresses, endpoints, accounts, senders, receivers, routes, credentials, gateway,
    provisioning: { tenants, endpoints, accounts, grants, routes } };
}
function event(phone = '100000000000001', id = 'wamid.synthetic-1', text = 'synthetic customer text') {
  return { object: 'whatsapp_business_account', entry: [{ id: WABA, changes: [{ field: 'messages', value: {
    metadata: { phone_number_id: phone }, messages: [{ id, from: '15550000001', type: 'text', text: { body: text } }],
  } }] }] };
}
function signed(value, stringify = JSON.stringify) {
  const raw = Buffer.isBuffer(value) ? value : Buffer.from(stringify(value));
  return { _raw: raw.toString('base64'), _signature: 'sha256=' + createHmac('sha256', SECRET).update(raw).digest('hex') };
}
async function actor(cleanups, script, input) {
  const child = fork(script, [], { stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    env: { PATH: process.env.PATH, NODE_ENV: 'test', TZ: 'UTC' } });
  const pending = new Map(); let sequence = 0, logs = '', done, crashed = false;
  const capture = b => { logs = (logs + b.toString()).slice(-200_000); };
  child.stdout.on('data', capture); child.stderr.on('data', capture);
  const exited = new Promise(resolve => child.once('exit', (code, signal) => {
    for (const p of pending.values()) { clearTimeout(p.deadline); p.reject(new Error('fixture actor exited')); }
    pending.clear(); resolve({ code, signal });
  }));
  child.on('message', message => {
    const p = pending.get(message.id); if (!p) return;
    pending.delete(message.id); clearTimeout(p.deadline);
    message.error ? p.reject(Object.assign(new Error(message.error), { _code: message.error })) : p.resolve(message.result);
  });
  function call(op, data = {}) {
    return new Promise((resolve, reject) => {
      if (!child.connected) { reject(new Error('fixture actor closed')); return; }
      const id = ++sequence;
      const deadline = setTimeout(() => { pending.delete(id); reject(new Error('fixture deadline: ' + op)); }, 15_000);
      pending.set(id, { resolve, reject, deadline }); child.send({ id, op, data });
    });
  }
  async function stop() {
    if (crashed) return;
    if (done) return done;
    done = (async () => {
      let stopped;
      try { if (child.connected) stopped = await call('stop'); }
      catch (error) { child.kill('SIGKILL'); await exited; throw error; }
      let deadline;
      const outcome = await Promise.race([exited, new Promise(resolve => { deadline = setTimeout(() => resolve(null), 2500); })]);
      clearTimeout(deadline);
      if (!outcome) { child.kill('SIGKILL'); await exited; assert.fail('fixture leaked handles after cleanup'); }
      assert.deepEqual(outcome, { code: 0, signal: null });
      return stopped;
    })(); return done;
  }
  cleanups.push(stop);
  const a = { call, stop, child, get logs() { return logs; },
    async crash() { crashed = true; child.kill('SIGKILL'); await exited; } };
  a.ready = await call('initialize', input);
  return a;
}
async function setup(t, overrides = {}, hostExtra = {}) {
  const dir = mkdtempSync(path.join(tmpdir(), 'xnet-provider-gateway-'));
  const f = provision(), cleanup = [], actors = [], clients = [];
  const db = path.join(dir, 'mailbox.sqlite'); let now = NOW, server;
  const limits = { _ttl_ms: 10_000, _registration_lease_ms: 1000, _claim_lease_ms: 200,
    _retry_base_ms: 10, _retry_max_ms: 20, _max_attempts: 3, ...overrides };
  const s = new SQLiteXNetStore({ _db_path: db, _claim_key: Buffer.alloc(32, 42), _limits: limits, _clock: () => now });
  try { s.initialize(); s.provision(f.provisioning); } finally { s.close(); }
  t.after(async () => { try {
    for (const fn of cleanup.reverse()) await fn();
    for (const a of actors) for (const value of [SECRET, ...f.credentials.map(c => c._token), 'synthetic customer text', 'sha256=']) {
      assert.equal(a.logs.includes(value), false, 'fixture logs must omit secrets and customer content');
    }
  }
    finally { rmSync(dir, { recursive: true, force: true }); } });
  const start = async extra => {
    server = await actor(cleanup, new URL('./provider-gateway-worker.mjs', import.meta.url), {
      dir, db, now, limits, provisioning: f.provisioning, credentials: f.credentials, gateway: f.gateway, ...extra });
    actors.push(server); return server;
  };
  await start(hostExtra);
  const e = { f, dir, db, actors, clients, get server() { return server; }, get now() { return now; },
    async ingest(i = 0, id = 'wamid.synthetic-1', text, extra = {}) {
      return server.call('ingest', { input: signed(event(f.accounts[i]._account_locator, id, text)), ...extra });
    },
    async status(message, i = 0) {
      const id = `status-${i}`;
      await server.call('probe', { id, auth: f.credentials[i]._token });
      return (await server.call('probe', { id, command: req('get-message-status', { message_id: message }) }))._result;
    },
    async client(i) {
      const c = await actor(cleanup, new URL('./transport-worker.mjs', import.meta.url), { role: 'client',
        ledger: path.join(dir, `target-${i}.sqlite`), address: f.addresses[i],
        url: `ws://127.0.0.1:${server.ready.address.port}/wh/v2`, token: f.credentials[i + 2]._token, now });
      clients[i] = c; actors.push(c); return c;
    },
    async frames(n = 1, delta = 10) {
      for (let i = 0; i < n; i++) {
        now += delta; await server.call('advance', { now });
        for (const c of clients) if (c?.child.connected) await c.call('advance', { now });
      }
    },
    async drive(message, i = 0) {
      for (let n = 0; n < 30; n++) {
        if ((await e.status(message, i))._state === 'processed') return;
        await e.frames();
      }
      assert.fail('no durable processing ACK within bounded explicit frames');
    },
    async restart(extra = {}) { await server.stop(); return start(extra); },
  };
  return e;
}

test('mock gateway: actual XNode routes two signed phones sharing a WABA, persists effects then processing ACKs', { timeout: 30_000 }, async t => {
  const e = await setup(t);
  assert.equal(e.server.ready.address.address, '127.0.0.1'); assert.ok(e.server.ready.address.port > 0);
  assert.equal(e.server.ready.loaded, true); assert.equal(e.server.ready.ready._ready, true);
  assert.equal(e.server.ready.connections, 2); assert.equal(e.server.ready.upgrades, 1);
  assert.equal((await e.server.call('inspect')).registrations.length, 0); // Producers never register endpoints.
  const a = await e.ingest(0), b = await e.ingest(1); // Identical provider event/customer ID, distinct trusted account.
  assert.equal(a._stage, 'durably_accepted'); assert.notEqual(a._message_id, b._message_id);
  assert.equal((await e.status(a._message_id))._state, 'queued');
  assert.equal((await e.status(a._message_id))._attempt_count, 0);
  const ca = await e.client(0), cb = await e.client(1);
  assert.equal(ca.ready.readiness._ready, true); assert.equal(cb.ready.readiness._ready, true);
  await e.drive(a._message_id); await e.drive(b._message_id, 1);
  for (const [i, c, acceptance] of [[0, ca, a], [1, cb, b]]) {
    const target = await c.call('inspect');
    assert.equal(target.effects.length, 1); assert.equal(target.receipts.length, 1);
    assert.equal(target.effects[0].tenant, e.f.tenants[i]); assert.equal(target.effects[0].endpoint, e.f.addresses[i]._endpoint_id);
    assert.equal(target.effects[0].message, acceptance._message_id);
    assert.equal((await e.status(acceptance._message_id, i))._receipt_id, target.receipts[0].id);
  }
  assert.equal((await e.server.call('inspect')).acks.length, 2);
});

test('mock gateway: malformed signatures, authority injection and unsupported input persist nothing', { timeout: 30_000 }, async t => {
  const e = await setup(t); const good = event();
  const invalid = [
    {}, { ...signed(good), _signature: 'sha256=' + '0'.repeat(64) },
    { ...signed(good), _signature: 'sha256=zz' },
    { ...signed(good), _raw: Buffer.from(JSON.stringify(event(undefined, 'tampered'))).toString('base64') },
    { ...signed(good), verified: true }, { ...signed(good), _tenant_id: e.f.tenants[1] },
    signed(Buffer.from('{')), signed(Buffer.alloc(256 * 1024 + 1, 32)),
  ];
  for (const input of invalid) await rejected(e.server.call('ingest', { input }));
  const mutate = fn => { const v = structuredClone(good); fn(v); return signed(v); };
  const cases = [
    v => { v.tenant_id = e.f.tenants[1]; }, v => { v.entry[0].id = '999'; },
    v => { delete v.entry[0].id; }, v => { v.entry[0].changes[0].value.metadata.phone_number_id = '999'; },
    v => { v.entry[0].changes[0].value._auth = { _authenticated: true }; },
    v => { v.entry[0].changes[0].value.metadata._endpoint_id = e.f.addresses[1]._endpoint_id; },
    v => { v.entry[0].changes[0].value.messages[0].type = 'image'; },
    v => { v.entry[0].changes[0].value.messages[0].text.body = ''; },
    v => { v.entry[0].changes[0].value.messages[0].text.body = 'x'.repeat(4097); },
    v => { v.entry[0].changes[0].value.messages[0].id = 'x'.repeat(513); },
    v => { v.entry[0].changes[0].value.messages[0].timestamp = '-1'; },
    v => { delete v.entry[0].changes[0].value.messages; v.entry[0].changes[0].value.statuses = []; },
    v => { v.entry[0].changes[0].value.messages[0].text.extra = { _module: 'ping', _op: 'ping' }; },
    v => { v.deep = Array.from({ length: 18 }).reduce(a => ({ nested: a }), {}); },
  ];
  for (const fn of cases) await rejected(e.server.call('ingest', { input: mutate(fn) }));
  await rejected(e.server.call('ingest', { input: signed(Buffer.from('{"object":"whatsapp_business_account","__proto__":{}}')) }));
  const snapshot = await e.server.call('inspect');
  for (const key of ['messages', 'deliveries', 'attempts', 'acks']) assert.equal(snapshot[key].length, 0);
  const literal = await e.ingest(0, 'literal-text', '{"_tenant_id":"foreign","_module":"ping","_op":"ping"}');
  assert.equal(literal._stage, 'durably_accepted'); // Text remains literal data, never a command or routing claim.
  assert.equal((await e.server.call('inspect')).messages[0].tenant_id, e.f.tenants[0]);
});

test('mock gateway: multi-event, multi-entry, mixed-account and valid-plus-invalid batches reject before persistence', { timeout: 30_000 }, async t => {
  const e = await setup(t);
  const a = event(), b = event(e.f.accounts[1]._account_locator);
  const inputs = [
    { ...a, entry: [...a.entry, ...b.entry] },
    { ...a, entry: [...a.entry, { id: '999', changes: [] }] },
    structuredClone(a), structuredClone(a), structuredClone(a),
  ];
  inputs[2].entry[0].changes[0].value.messages.push({ ...a.entry[0].changes[0].value.messages[0], id: 'second' });
  inputs[3].entry[0].changes[0].value.messages.push({ type: 'image' });
  inputs[4].entry[0].changes.push(b.entry[0].changes[0]);
  for (const value of inputs) await rejected(e.server.call('ingest', { input: signed(value) }));
  assert.equal((await e.server.call('inspect')).messages.length, 0);
});

test('mock gateway: provisioning rejects ambiguous accounts, wrong principals, routes and excessive permissions', () => {
  const f = provision();
  const config = { ...f.gateway, _provisioning: f.provisioning, _credentials: f.credentials };
  assert.doesNotThrow(() => validateBindings(config));
  const mutations = [
    c => { c._bindings[1]._receiving_phone_id = c._bindings[0]._receiving_phone_id; },
    c => { c._bindings[0]._account_binding_id = c._bindings[1]._account_binding_id; },
    c => { c._bindings[0]._principal_id = c._bindings[1]._principal_id; },
    c => { c._bindings[0]._credential_ref = c._bindings[1]._credential_ref; },
    c => { c._bindings[0]._profile_id = 'unknown'; },
    c => { c._provisioning.accounts[0]._enabled = false; },
    c => { c._provisioning.routes[0]._destination = c._provisioning.endpoints[1]._address; },
    c => { c._provisioning.routes[0]._enabled = false; },
    c => { c._provisioning.grants[0]._permissions.push('receive'); },
    c => { c._provisioning.grants[1]._permissions.push('submit'); },
    c => { c._credentials[0]._principal = c._credentials[1]._principal; },
    c => { c._credentials[0]._token = c._credentials[1]._token; },
    c => { c._provisioning.accounts.push({ ...c._provisioning.accounts[0], _id: randomUUID(), _adapter_id: 'another-adapter' }); },
  ];
  for (const mutate of mutations) {
    const bad = structuredClone(config); mutate(bad);
    assert.throws(() => validateBindings(bad), error => error._code === 'MOCK_INVALID_BINDING');
  }
});

test('mock gateway: existing authenticated gateway independently enforces producer/endpoint grants and tenant isolation', { timeout: 30_000 }, async t => {
  const e = await setup(t), message = await e.ingest();
  const call = (id, command, extra = {}) => e.server.call('probe', { id, command, ...extra });
  assert.equal((await call('anonymous', req('get-readiness'), { unauth: true }))._ok, false);
  assert.equal((await e.server.call('probe', { id: 'bad', auth: 'synthetic-invalid-credential' }))._ok, false);
  for (const [i, c] of e.f.credentials.entries()) assert.equal((await e.server.call('probe', { id: `p${i}`, auth: c._token }))._ok, true);
  for (const command of [req('register-endpoint', { address: e.f.addresses[0] }), req('receive-next', {}), req('ack-processed', {}),
    { _module: 'ping', _op: 'ping', _params: {} }, req('submit', {}, { _ctx: { _auth: { _authenticated: true } } })]) {
    await rejected(call('p0', command));
  }
  const intent = { message_type: TYPE, idempotency_key: 'unauthorized', event_id: 'unauthorized',
    payload: { _mode: 'inline', _content_type: 'application/json', _schema_id: 'mock.whatsapp.inbound.v1', _data: {} } };
  await rejected(call('p2', req('submit', intent)));
  await rejected(call('p0', req('submit', { ...intent, destination: e.f.addresses[1] })));
  await rejected(call('p0', req('submit', { ...intent, _tenant_id: e.f.tenants[1] })));
  await rejected(call('p1', req('get-message-status', { message_id: message._message_id })));
  await rejected(call('p3', req('get-message-status', { message_id: message._message_id })));
  await rejected(call('p3', req('register-endpoint', { address: e.f.addresses[0] })));
  await e.server.call('revoke', { ref: 'producer-0' });
  await rejected(e.ingest(0, 'revoked')); await rejected(e.ingest());
  assert.equal((await e.server.call('inspect')).messages.length, 1);
});

test('mock gateway: changed stored source/grant/route/endpoint policy fails closed without rewriting duplicate history', { timeout: 30_000 }, async t => {
  const e = await setup(t), original = await e.ingest();
  const before = await e.server.call('inspect');
  for (const [kind, id] of [['route', e.f.routes[0]._id], ['endpoint', e.f.addresses[0]._endpoint_id],
    ['account', e.f.accounts[0]._id], ['grant', e.f.senders[0]._principal_id]]) {
    await e.server.call('disable', { kind, id });
    await rejected(e.ingest(0, `disabled-${kind}`));
    if (['route', 'endpoint'].includes(kind)) assert.equal((await e.ingest())._message_id, original._message_id);
    else await rejected(e.ingest());
    await e.server.call('disable', { kind, id, enabled: true });
  }
  const after = await e.server.call('inspect');
  assert.deepEqual(after.messages, before.messages); assert.deepEqual(after.deliveries, before.deliveries);
});

test('mock gateway: concurrent/reformatted replay uses stable bounded keys; conflicts never create new effects', { timeout: 30_000 }, async t => {
  const e = await setup(t), value = event(undefined, 'x'.repeat(512), 'x'.repeat(4096));
  value.entry[0].changes[0].value.messages[0].timestamp = '1000';
  value.entry[0].changes[0].value.messages[0].context = { id: 'reply-1' };
  const responses = await e.server.call('concurrent-ingest', { inputs: [signed(value), signed(value)] });
  assert.equal(responses[0]._message_id, responses[1]._message_id);
  assert.equal(responses.filter(a => !a._duplicate).length, 1);
  const reformatted = { entry: value.entry, object: value.object };
  const replay = await e.server.call('ingest', { input: signed(reformatted, v => JSON.stringify(v, null, 2)) });
  assert.equal(replay._message_id, responses[0]._message_id); assert.equal(replay._duplicate, true);
  const before = await e.server.call('inspect'); const envelope = JSON.parse(before.messages[0].envelope);
  assert.equal(envelope._event_id.length, 70); assert.equal(envelope._payload._data._external_message_id.length, 512);
  assert.equal(envelope._payload._data._provider_timestamp_ms, 1_000_000);
  assert.equal(envelope._payload._data._reply_to_external_message_id, 'reply-1');
  value.entry[0].changes[0].value.messages[0].text.body = 'changed';
  await assert.rejects(e.server.call('ingest', { input: signed(value) }), e => e._code === 'E_XNET_IDEMPOTENCY_CONFLICT');
  assert.deepEqual((await e.server.call('inspect')).messages, before.messages);
  const c = await e.client(0); await e.drive(replay._message_id);
  assert.equal((await c.call('inspect')).effects.length, 1);
});

test('mock gateway: transaction rollback and lost durable acceptance response recover with the identical event', { timeout: 30_000 }, async t => {
  const e = await setup(t);
  for (const point of ['accept.after-message', 'accept.after-delivery']) {
    await e.server.call('fault', { point }); await rejected(e.ingest());
    const state = await e.server.call('inspect'); assert.equal(state.messages.length, 0); assert.equal(state.deliveries.length, 0);
  }
  await e.server.call('fault', {}); await e.server.call('drop', { acceptance: 1 });
  await rejected(e.ingest());
  const committed = await e.server.call('inspect'); assert.equal(committed.messages.length, 1);
  assert.equal(committed.dropped.acceptance, 1);
  const replay = await e.ingest(); assert.equal(replay._duplicate, true);
  assert.equal(replay._message_id, committed.messages[0].id);
  const c = await e.client(0); await e.drive(replay._message_id);
  assert.equal((await c.call('inspect')).effects.length, 1);
});

test('mock gateway: offline acceptance and fresh-process restart preserve deduplication across TTL/route-policy changes', { timeout: 30_000 }, async t => {
  const e = await setup(t), accepted = await e.ingest();
  const before = await e.server.call('inspect');
  assert.equal(before.deliveries[0].attempt_count, 0); assert.equal(before.deliveries[0].state, 'queued');
  await e.restart({ limits: { _ttl_ms: 20_000, _registration_lease_ms: 1000, _claim_lease_ms: 200 } });
  await e.server.call('disable', { kind: 'route', id: e.f.routes[0]._id });
  const replay = await e.ingest(); assert.equal(replay._duplicate, true);
  assert.equal(replay._message_id, accepted._message_id); assert.equal(replay._expires_at, accepted._expires_at);
  const afterReplay = await e.server.call('inspect');
  assert.deepEqual(afterReplay.messages, before.messages); assert.deepEqual(afterReplay.deliveries, before.deliveries);
  await e.server.call('disable', { kind: 'route', id: e.f.routes[0]._id, enabled: true });
  const c = await e.client(0); await e.drive(accepted._message_id);
  assert.equal((await c.call('inspect')).effects.length, 1);
});

test('mock gateway: lost receive and ACK responses retain one attempt and one atomic target effect', { timeout: 30_000 }, async t => {
  const e = await setup(t), c = await e.client(0);
  await e.server.call('drop', { receive: 1, ack: 1 });
  const accepted = await e.ingest();
  await e.drive(accepted._message_id);
  const state = await e.server.call('inspect'), target = await c.call('inspect');
  assert.equal(state.dropped.receive, 1); assert.equal(state.dropped.ack, 1);
  assert.equal(target.effects.length, 1); assert.equal(target.receipts.length, 1);
  assert.equal(state.deliveries[0].attempt_count, 1); assert.equal(state.acks.length, 1);
  await e.frames(3); assert.equal((await c.call('inspect')).effects.length, 1);
});

test('mock gateway: target and host crashes after atomic effect/receipt recover without duplicate business effects', { timeout: 30_000 }, async t => {
  const e = await setup(t), c = await e.client(0), accepted = await e.ingest();
  for (let n = 0; n < 20; n++) { await e.frames(); if ((await c.call('inspect')).effects.length) break; }
  const target = await c.call('inspect'); assert.equal(target.effects.length, 1); assert.equal(target.receipts.length, 1);
  assert.equal((await e.status(accepted._message_id))._state, 'dispatched');
  await c.crash(); await e.server.crash(); await e.restart();
  assert.equal((await e.ingest())._message_id, accepted._message_id);
  await e.frames(1, 210);
  const restarted = await e.client(0); await e.drive(accepted._message_id);
  const recovered = await restarted.call('inspect');
  assert.deepEqual(recovered.effects, target.effects); assert.deepEqual(recovered.receipts, target.receipts);
  assert.equal((await e.status(accepted._message_id))._attempt_count, 2);
});

test('mock gateway: uncertain work is parked; explicit shutdown fences ingress and releases owned runtime resources', { timeout: 30_000 }, async t => {
  const e = await setup(t), c = await e.client(0); await c.call('uncertain');
  const accepted = await e.ingest(); await e.frames(5);
  assert.equal((await e.status(accepted._message_id))._state, 'uncertain');
  const calls = (await c.call('inspect')).calls; await e.frames(5, 500);
  assert.equal((await c.call('inspect')).calls, calls);
  assert.equal((await c.call('inspect')).effects.length, 0);
  await e.server.call('stop-gateway'); await rejected(e.ingest(0, 'after-stop'));
  await c.stop();
  assert.deepEqual(await e.server.stop(), { sameStop: true, frameStable: true, coreRunning: false,
    listening: false, connections: 0, upgradeListeners: 0 });
  for (const a of e.actors) for (const secret of [SECRET, ...e.f.credentials.map(c => c._token), 'synthetic customer text', 'sha256=']) {
    assert.equal(a.logs.includes(secret), false);
  }
});

test('mock gateway: lost availability hint is recovered by later explicit host advancement', { timeout: 30_000 }, async t => {
  const e = await setup(t), c = await e.client(0);
  await e.server.call('drop', { available: 1 });
  const accepted = await e.ingest();
  await e.frames();
  assert.equal((await e.server.call('inspect')).dropped.available, 1);
  await e.drive(accepted._message_id);
  assert.equal((await c.call('inspect')).effects.length, 1);
});

test('mock gateway: target receipt insertion failure rolls back the effect and parks uncertainty', { timeout: 30_000 }, async t => {
  const e = await setup(t);
  // Inject a real SQLite failure between the target's effect insert and receipt insert.
  const ledger = new Database(path.join(e.dir, 'target-0.sqlite'));
  try { ledger.exec(`CREATE TABLE receipt(tenant TEXT,endpoint TEXT,message TEXT,id TEXT,idem TEXT,processed_at INTEGER,PRIMARY KEY(tenant,endpoint,message));
    CREATE TABLE effect(message TEXT PRIMARY KEY,tenant TEXT,endpoint TEXT);
    CREATE TRIGGER fail_receipt BEFORE INSERT ON receipt BEGIN SELECT RAISE(ABORT,'synthetic receipt failure'); END;`); }
  finally { ledger.close(); }
  const c = await e.client(0), accepted = await e.ingest(); await e.frames(5);
  assert.equal((await e.status(accepted._message_id))._state, 'uncertain');
  const state = await c.call('inspect'); assert.equal(state.calls, 1);
  assert.equal(state.effects.length, 0); assert.equal(state.receipts.length, 0);
  await e.frames(5, 500); assert.equal((await c.call('inspect')).calls, 1);
});

test('mock gateway: partial producer authentication failure and identity mismatch clean up the real XNode', { timeout: 30_000 }, async t => {
  // Each failed start must still exit naturally after public cleanup; the actor guard rejects leaked handles.
  await assert.rejects(setup(t, {}, { reject_credential: 'producer-1' }), e => e._code === 'MOCK_AUTH_FAILED');
  await assert.rejects(setup(t, {}, { mismatch_credential: 'producer-0' }), e => e._code === 'MOCK_AUTH_FAILED');
});
