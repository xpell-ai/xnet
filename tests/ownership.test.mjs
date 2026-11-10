import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import Database from 'better-sqlite3';
import { createXNetModule, createXNetWormholesHost } from '@xpell/xnet/server';
import { fixture, rejects } from './fixtures.mjs';

const bound = (f, i = 0) => ({ ...f.receivers[i], _connection_id: randomUUID(), _session_id: randomUUID(), _auth_expires_at: f.now + 1000 });

test('connection-bound registrations cannot be used with bare principals or forged sessions', t => {
  const f = fixture(t); const p = bound(f); const reg = f.store.registerEndpoint(p, f.addresses[0]);
  f.store.accept(f.senders[0], f.intent());
  for (const bad of [f.receivers[0], { ...p, _connection_id: randomUUID() }, { ...p, _session_id: randomUUID() }, bound(f, 1)]) {
    rejects(assert, () => f.store.claimNext(bad, reg._id, 'steal'), 'E_XNET_STALE_CLAIM');
    rejects(assert, () => f.store.renewRegistration(bad, reg._id, reg._owner_epoch), 'E_XNET_STALE_CLAIM');
    rejects(assert, () => f.store.unregisterEndpoint(bad, reg._id, reg._owner_epoch), 'E_XNET_STALE_CLAIM');
  }
  const d = f.store.claimNext(p, reg._id, 'owner'); f.store.markDispatched(p, d);
  assert.equal(f.store.acknowledge(p, f.ack(d))._state, 'processed');
});

test('registration renewal preserves owner epoch and never extends an existing claim', t => {
  const f = fixture(t, { _registration_lease_ms: 30, _claim_lease_ms: 50 }); const p = bound(f);
  const reg = f.store.registerEndpoint(p, f.addresses[0]); f.store.accept(f.senders[0], f.intent());
  const d = f.store.claimNext(p, reg._id, 'original'); f.store.markDispatched(p, d);
  f.now += 10; const renewed = f.store.renewRegistration(p, reg._id, reg._owner_epoch);
  assert.equal(renewed._id, reg._id); assert.equal(renewed._owner_epoch, reg._owner_epoch);
  assert.equal(renewed._lease_expires_at, reg._lease_expires_at + 10);
  assert.equal(f.store.claimNext(p, reg._id, 'original')._claim_expires_at, d._claim_expires_at);
  f.now = d._claim_expires_at;
  rejects(assert, () => f.store.acknowledge(p, f.ack(d)), 'E_XNET_STALE_CLAIM');
});

test('credential expiry caps registration, renewal and every new claim deadline', t => {
  const f = fixture(t); const p = { ...bound(f), _auth_expires_at: f.now + 15 };
  const reg = f.store.registerEndpoint(p, f.addresses[0]); assert.equal(reg._lease_expires_at, p._auth_expires_at);
  f.store.accept(f.senders[0], f.intent()); f.now += 10;
  assert.equal(f.store.renewRegistration(p, reg._id, reg._owner_epoch)._lease_expires_at, p._auth_expires_at);
  const d = f.store.claimNext(p, reg._id, 'bounded'); assert.equal(d._claim_expires_at, p._auth_expires_at);
  f.now += 5; rejects(assert, () => f.store.authorizePrincipal(p, 'receive'), 'E_XNET_FORBIDDEN');
});

test('stored grant revocation blocks receive, ACK, renewal, status and unregister on an existing connection', t => {
  const f = fixture(t); const p = bound(f); const reg = f.store.registerEndpoint(p, f.addresses[0]);
  const a = f.store.accept(f.senders[0], f.intent()); const d = f.store.claimNext(p, reg._id, 'claim'); f.store.markDispatched(p, d);
  const db = new Database(f.options._db_path); const g = f.provisioning.grants.find(g => g._principal_id === p._principal_id);
  db.prepare('UPDATE xnet_grant SET definition=? WHERE tenant_id=? AND principal_id=?').run(JSON.stringify({ ...g, _enabled: false }), p._tenant_id, p._principal_id); db.close();
  for (const operation of [() => f.store.claimNext(p, reg._id, 'claim'), () => f.store.acknowledge(p, f.ack(d)),
    () => f.store.renewRegistration(p, reg._id, reg._owner_epoch), () => f.store.getStatus(p, a._message_id),
    () => f.store.unregisterEndpoint(p, reg._id, reg._owner_epoch)]) rejects(assert, operation, 'E_XNET_FORBIDDEN');
  assert.equal(f.store.getStatus(f.senders[0], a._message_id)._state, 'dispatched');
});

test('async module frame work is nonoverlapping and stop prevents another advancement', async t => {
  const f = fixture(t); let release, entered; let passes = 0;
  const started = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const module = createXNetModule({ _store: f.store, _authorize: () => f.receivers[0], _advance_on_frame: true,
    _frame_work: async () => { passes++; entered(); await gate; } });
  module.start(); const first = module.onFrame(1); await started;
  await module.onFrame(2); assert.equal(passes, 1);
  module.stop(); release(); await first; await module.onFrame(3); assert.equal(passes, 1);
});

test('host refuses to attach a competing upgrade gateway without opening transport', async t => {
  const f = fixture(t); const server = createServer(); server.on('upgrade', () => {});
  const host = createXNetWormholesHost({ _store: f.store, _verify_credential: () => undefined });
  await assert.rejects(host.start(server), e => e._code === 'E_XNET_INVALID_CONFIG');
  assert.equal(server.listening, false);
});
