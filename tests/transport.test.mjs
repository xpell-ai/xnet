import assert from 'node:assert/strict';
import test from 'node:test';
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { fixture, TYPE } from './fixtures.mjs';

async function runtime(t, input) {
  const child = fork(new URL('./transport-worker.mjs', import.meta.url), [], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let sequence = 0; const pending = new Map(); let errors = '';
  child.stderr.on('data', bytes => { errors += bytes.toString().slice(0, 2000); });
  child.on('message', message => {
    const operation = pending.get(message.id); if (!operation) return;
    pending.delete(message.id); clearTimeout(operation.timer);
    message.error ? operation.reject(Object.assign(new Error(message.error), { _code: message.error })) : operation.resolve(message.result);
  });
  const exited = new Promise(resolve => child.once('exit', resolve));
  child.once('exit', () => { for (const p of pending.values()) { clearTimeout(p.timer); p.reject(new Error(`test worker exited: ${errors}`)); } pending.clear(); });
  function call(op, data = {}) {
    return new Promise((resolve, reject) => {
      if (!child.connected) { reject(new Error('test runtime channel closed')); return; }
      const id = ++sequence;
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`test runtime timeout: ${op}`)); }, 15_000);
      pending.set(id, { resolve, reject, timer });
      child.send({ id, op, data }, error => {
        if (!error) return;
        const operation = pending.get(id); if (!operation) return;
        pending.delete(id); clearTimeout(operation.timer); operation.reject(new Error('test runtime channel closed'));
      });
    });
  }
  t.after(async () => {
    if (child.connected && child.exitCode === null && child.signalCode === null) {
      try { await call('stop'); } catch { child.kill('SIGKILL'); }
    }
    await exited;
  });
  const ready = await call('initialize', input);
  return { call, ready, child, async crash() { child.kill('SIGKILL'); await exited; } };
}
async function setup(t, options = {}) {
  const cleanup = [];
  const childContext = { after: fn => cleanup.push(fn) };
  const fixtureCleanup = [];
  const f = fixture({ after: fn => fixtureCleanup.push(fn) }, { _ttl_ms: 10_000, _registration_lease_ms: 1000, _claim_lease_ms: 200, _retry_base_ms: 10 },
    options.allow_prototype ? [TYPE, 'constructor'] : [TYPE]);
  if (options.allow_prototype) f.store.setRouteBinding({ ...f.routes[0], _id: randomUUID(), _message_type: 'constructor' }, 0);
  t.after(async () => { for (const fn of cleanup.reverse()) await fn(); for (const fn of fixtureCleanup) fn(); });
  f.store.close();
  let now = f.now;
  const tokens = ['synthetic-agent-a-secret', 'synthetic-agent-b-secret'];
  const phone_ids = f.accounts.map(a => a._account_locator);
  const hostInput = { role: 'server', db: f.options._db_path, limits: f.options._limits, now,
    tokens, phone_ids, senders: f.senders, receivers: f.receivers, ...options };
  let server = await runtime(childContext, hostInput);
  const port = server.ready.port;
  const clients = [];
  const input = i => ({ role: 'client', ledger: path.join(f.dir, `target-${i}.sqlite`),
    url: `ws://127.0.0.1:${port}/wh/v2`, address: f.addresses[i], token: tokens[i], now });
  const env = { f, tokens, phone_ids, clients,
    get server() { return server; }, get now() { return now; },
    async client(i, extra = {}) { const c = await runtime(childContext, { ...input(i), ...extra }); clients[i] = c; return c; },
    async restartServer() { server = await runtime(childContext, { ...hostInput, now, port }); return server; },
    async add(i, event, extra = {}) { return server.call('accept', { signature: 'mock-verified', phone_id: phone_ids[i], event, now, ...extra }); },
    async status(message, tenant = 0) { return server.call('status', { message, tenant, now }); },
    async frames(count = 1, delta = 10) {
      for (let n = 0; n < count; n++) {
        now += delta; await server.call('advance', { now });
        for (const c of clients) if (c && c.child.connected && c.child.exitCode === null && c.child.signalCode === null) await c.call('advance', { now });
      }
    },
    async jump(delta) { now += delta; await server.call('advance', { now }); },
  };
  return env;
}
const req = (op, params = {}, extra = {}) => ({ _module: 'xnet', _op: op, _params: params, ...extra });
async function targetEffect(e, client) {
  for (let n = 0; n < 10; n++) {
    if ((await client.call('inspect')).effects.length) return;
    await e.frames(1);
  }
  assert.fail('target did not receive its message in bounded explicit frames');
}
async function processed(e, message, tenant = 0) {
  for (let n = 0; n < 20; n++) {
    if ((await e.status(message, tenant))._state === 'processed') return;
    await e.frames(1);
  }
  assert.fail('message did not reach a durable processed ACK within bounded explicit frames');
}

test('two authenticated outbound endpoint processes route by trusted account, ACK durably and isolate tenants', { timeout: 30_000 }, async t => {
  const e = await setup(t); const a = await e.client(0); const b = await e.client(1);
  assert.equal(a.ready.readiness._ready, true); assert.equal(b.ready.readiness._ready, true);
  const messageA = await e.add(0, 'two-a', { payload: { _tenant_id: e.f.tenants[1], _endpoint_id: e.f.addresses[1]._endpoint_id,
    _module: 'unrelated', _op: 'execute', _wid: 'spoof', _sid: 'spoof', _auth: { _authenticated: true } } });
  const messageB = await e.add(1, 'two-a'); // Provider event identity is scoped to the trusted receiving account.
  assert.equal((await e.add(0, 'two-a', { payload: { _tenant_id: e.f.tenants[1], _endpoint_id: e.f.addresses[1]._endpoint_id,
    _module: 'unrelated', _op: 'execute', _wid: 'spoof', _sid: 'spoof', _auth: { _authenticated: true } } }))._message_id, messageA._message_id);
  await e.frames(4);
  const aa = await a.call('inspect'); const bb = await b.call('inspect');
  assert.equal(aa.effects.length, 1); assert.equal(bb.effects.length, 1);
  assert.ok(aa.effects.every(row => row.tenant === e.f.tenants[0] && row.message === messageA._message_id));
  assert.ok(bb.effects.every(row => row.tenant === e.f.tenants[1] && row.message === messageB._message_id));
  assert.equal((await e.status(messageA._message_id))._state, 'processed');
  assert.equal((await e.status(messageB._message_id, 1))._state, 'processed');
  await assert.rejects(e.server.call('accept', { signature: 'bad', phone_id: e.phone_ids[0], event: 'bad' }), x => x._code === 'MOCK_BAD_SIGNATURE');
  await assert.rejects(e.server.call('accept', { signature: 'mock-verified', phone_id: 'unknown', event: 'bad' }), x => x._code === 'MOCK_UNBOUND_ACCOUNT');
});

test('lost availability hints do not strand idle clients; renewal preserves owner and claim fencing', { timeout: 30_000 }, async t => {
  const e = await setup(t); const c = await e.client(0); const original = c.ready.registration;
  await e.server.call('drop', { available: 1 }); const accepted = await e.add(0, 'hint-lost');
  await e.frames(1, 20); assert.equal((await c.call('inspect')).effects.length, 0);
  await e.frames(4, 20); assert.equal((await c.call('inspect')).effects.length, 1);
  assert.equal((await e.status(accepted._message_id))._state, 'processed');
  await e.frames(1, 300);
  const renewed = (await c.call('inspect')).registration;
  assert.equal(renewed._id, original._id); assert.equal(renewed._owner_epoch, original._owner_epoch);
  assert.ok(renewed._lease_expires_at > original._lease_expires_at);
});

test('offline recipients drain after fresh authentication and connection replacement', { timeout: 30_000 }, async t => {
  const e = await setup(t); const accepted = await e.add(0, 'offline');
  assert.equal((await e.status(accepted._message_id))._attempt_count, 0);
  const c = await e.client(0); const before = c.ready.registration;
  await e.frames(2); assert.equal((await e.status(accepted._message_id))._state, 'processed');
  await e.server.call('disconnect', { tenant: 0 });
  await e.frames(5, 20);
  const after = (await c.call('inspect')).registration;
  assert.ok(after && after._owner_epoch > before._owner_epoch);
  assert.notEqual(after._id, before._id); assert.notEqual(after._connection_id, before._connection_id);
  const again = await e.add(0, 'after-reconnect'); await processed(e, again._message_id);
  assert.equal((await e.status(again._message_id))._state, 'processed');
});

test('lost receive response reuses the committed attempt; lost ACK response is idempotent', { timeout: 30_000 }, async t => {
  const e = await setup(t); const c = await e.client(0);
  const accepted = await e.add(0, 'lost-receive'); await e.server.call('drop', { receive: 1 });
  for (let n = 0; n < 10; n++) { await e.frames(1); if ((await e.status(accepted._message_id))._state === 'dispatched') break; }
  assert.equal((await c.call('inspect')).effects.length, 0);
  assert.equal((await e.status(accepted._message_id))._attempt_count, 1);
  // The transport timeout uses its existing wall-clock sweep; the injected lifecycle clock has not expired the claim.
  await targetEffect(e, c); assert.equal((await c.call('inspect')).effects.length, 1);
  await e.server.call('drop', { ack: 1 }); await e.frames(1);
  assert.equal((await e.status(accepted._message_id))._state, 'processed');
  await e.frames(2);
  const state = await c.call('inspect'); assert.equal(state.calls, 1); assert.equal(state.effects.length, 1);
  const status = await e.status(accepted._message_id); assert.equal(status._attempt_count, 1);
  assert.equal(status._receipt_id, state.receipts[0].id);
});

test('client crash after durable target receipt causes redelivery without a second business effect', { timeout: 30_000 }, async t => {
  const e = await setup(t); const c = await e.client(0); const accepted = await e.add(0, 'client-crash');
  await targetEffect(e, c); assert.equal((await c.call('inspect')).effects.length, 1);
  assert.equal((await e.status(accepted._message_id))._state, 'dispatched');
  await c.crash(); await e.jump(210); await e.frames(1, 10);
  const restarted = await e.client(0); await e.frames(4, 10);
  const state = await restarted.call('inspect'); assert.equal(state.effects.length, 1); assert.equal(state.receipts.length, 1);
  const status = await e.status(accepted._message_id); assert.equal(status._state, 'processed'); assert.equal(status._attempt_count, 2);
});

test('server process restart preserves queued work, fences old connections and recovers outstanding receipts', { timeout: 30_000 }, async t => {
  const e = await setup(t); const c = await e.client(0); const accepted = await e.add(0, 'server-crash');
  await targetEffect(e, c); assert.equal((await c.call('inspect')).effects.length, 1);
  const oldRegistration = (await c.call('inspect')).registration;
  await e.server.crash(); await e.restartServer();
  await e.frames(8, 40);
  const state = await c.call('inspect'); assert.equal(state.effects.length, 1);
  assert.ok(state.registration._owner_epoch > oldRegistration._owner_epoch);
  assert.equal((await e.status(accepted._message_id))._state, 'processed');
});

test('gateway rejects unauthenticated, unrelated, forged and cross-tenant operations', { timeout: 30_000 }, async t => {
  const e = await setup(t); const accepted = await e.add(0, 'auth-boundary');
  const unauth = await e.server.call('probe', { id: 'anonymous', unauth: true, command: req('get-readiness') });
  assert.equal(unauth._ok, false);
  assert.equal((await e.server.call('probe', { id: 'bad', auth: 'invalid-secret' }))._ok, false);
  assert.equal((await e.server.call('probe', { id: 'a', auth: e.tokens[0] }))._ok, true);
  assert.equal((await e.server.call('probe', { id: 'b', auth: e.tokens[1] }))._ok, true);
  for (const command of [
    { _module: 'ping', _op: 'ping', _params: {} }, req('provision-endpoint'), req('_submit'),
    req('get-readiness', {}, { _output: { _target: 'xdata', _key: 'unsafe' } }),
    req('register-endpoint', { address: e.f.addresses[1] }),
  ]) await assert.rejects(e.server.call('probe', { id: 'a', command }));
  await assert.rejects(e.server.call('probe', { id: 'b', command: req('get-message-status', { message_id: accepted._message_id }) }), x => x._code === 'E_XNET_FORBIDDEN');
  const registration = (await e.server.call('probe', { id: 'a', command: req('register-endpoint', { address: e.f.addresses[0], _wid: 'forged', _sid: 'forged', _auth: { _authenticated: true } }) }))._result;
  assert.notEqual(registration._connection_id, 'forged'); assert.notEqual(registration._session_id, 'forged');
  await assert.rejects(e.server.call('probe', { id: 'b', command: req('receive-next', { registration_id: registration._id, request_key: 'steal' }) }));
  await e.server.call('revoke', { tenant: 0, grant: true });
  for (const command of [req('get-readiness'), req('get-message-status', { message_id: accepted._message_id }),
    req('renew-registration', { registration_id: registration._id, owner_epoch: registration._owner_epoch })]) {
    await assert.rejects(e.server.call('probe', { id: 'a', command }), x => x._code === 'E_XNET_FORBIDDEN');
  }
});

test('competing connections cannot own an endpoint; expired registrations and stale ACKs fail closed', { timeout: 30_000 }, async t => {
  const e = await setup(t); await e.add(0, 'ownership');
  await e.server.call('probe', { id: 'first', auth: e.tokens[0] });
  await e.server.call('probe', { id: 'competitor', auth: e.tokens[0] });
  const reg = (await e.server.call('probe', { id: 'first', command: req('register-endpoint', { address: e.f.addresses[0] }) }))._result;
  await assert.rejects(e.server.call('probe', { id: 'competitor', command: req('register-endpoint', { address: e.f.addresses[0] }) }), x => x._code === 'E_XNET_OWNER_CONFLICT');
  const d = (await e.server.call('probe', { id: 'first', command: req('receive-next', { registration_id: reg._id, request_key: 'claim' }) }))._result;
  const ack = { delivery_id: d._delivery_id, message_id: d._envelope._message_id, attempt_id: d._attempt_id,
    registration_id: d._registration_id, owner_epoch: d._owner_epoch, claim_token: d._claim_token, outcome: 'processed', receipt_id: randomUUID() };
  await assert.rejects(e.server.call('probe', { id: 'competitor', command: req('ack-processed', ack) }), x => x._code === 'E_XNET_STALE_CLAIM');
  await e.jump(1001);
  await assert.rejects(e.server.call('probe', { id: 'first', command: req('renew-registration', { registration_id: reg._id, owner_epoch: reg._owner_epoch }) }), x => x._code === 'E_XNET_STALE_CLAIM');
  const replacement = (await e.server.call('probe', { id: 'competitor', command: req('register-endpoint', { address: e.f.addresses[0] }) }))._result;
  assert.ok(replacement._owner_epoch > reg._owner_epoch);
  await assert.rejects(e.server.call('probe', { id: 'first', command: req('ack-processed', ack) }), x => x._code === 'E_XNET_STALE_CLAIM');
});

test('uncertain handler results are durably parked; credential revocation prevents further endpoint operations', { timeout: 30_000 }, async t => {
  const e = await setup(t); const c = await e.client(0); await c.call('uncertain');
  const accepted = await e.add(0, 'uncertain'); await e.frames(4);
  assert.equal((await e.status(accepted._message_id))._state, 'uncertain');
  const before = await c.call('inspect'); await e.frames(4, 500);
  assert.equal((await c.call('inspect')).calls, before.calls);
  assert.equal((await e.status(accepted._message_id))._state, 'uncertain');
  await e.server.call('revoke', { tenant: 0, credential: true }); await e.frames(3, 10);
  assert.equal((await c.call('inspect')).readiness._ready, false);
});

test('bounded round-robin availability reaches both tenants when the frame budget is one', { timeout: 30_000 }, async t => {
  const e = await setup(t, { frame_limit: 1 }); const a = await e.client(0); const b = await e.client(1);
  await e.add(0, 'fair-a'); await e.add(1, 'fair-b'); await e.frames(8);
  assert.equal((await a.call('inspect')).effects.length, 1); assert.equal((await b.call('inspect')).effects.length, 1);
});

test('unallowlisted handlers never execute and foreign receipt assertions become uncertain', { timeout: 30_000 }, async t => {
  const e = await setup(t, { allow_prototype: true }); const a = await e.client(0, { no_handler: true });
  const b = await e.client(1, { foreign_tenant: e.f.tenants[0] });
  const unknown = await e.add(0, 'no-handler', { payload: { _module: 'sim-agent', _op: 'process-message' } });
  const forgedReceipt = await e.add(1, 'foreign-receipt'); await e.frames(5);
  assert.equal((await a.call('inspect')).calls, 0); assert.equal((await a.call('inspect')).effects.length, 0);
  assert.equal((await e.status(unknown._message_id))._state, 'dead_letter');
  const inherited = await e.add(0, 'no-inherited-handler', { message_type: 'constructor' });
  await e.frames(5);
  assert.equal((await a.call('inspect')).calls, 0);
  assert.equal((await e.status(inherited._message_id))._state, 'dead_letter');
  assert.equal((await b.call('inspect')).effects.length, 1);
  assert.equal((await e.status(forgedReceipt._message_id, 1))._state, 'uncertain');
});

test('explicit client stop cancels a pending receive and waits for lifecycle completion', { timeout: 30_000 }, async t => {
  const e = await setup(t); const c = await e.client(0);
  const accepted = await e.add(0, 'stop-pending'); await e.server.call('drop', { receive: 1 });
  const dropped = e.server.call('wait-drop', { kind: 'receive' });
  let stopping = false;
  const draining = (async () => { for (let n = 0; n < 20 && !stopping; n++) await e.frames(1); })().catch(() => {});
  await dropped; // Synchronize on the real committed response being lost, before its timeout.
  stopping = true;
  await c.call('stop'); await draining;
  assert.equal((await e.status(accepted._message_id))._state, 'dispatched');
});
