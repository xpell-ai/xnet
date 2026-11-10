import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { _x, _xd, XCommand } from '@xpell/core';
import {
  assertXNetAddress, assertXNetEndpointIdentity, assertXNetRegistration, assertXNetBindingSource,
  assertXNetTrustedSource, assertXNetRouteBinding, assertXNetEnvelope, assertXNetSubmitIntent,
  assertXNetAcceptance, assertXNetDispatch, assertXNetAttempt, assertXNetDeliveryState,
  assertXNetProcessingAck, assertXNetTargetReceipt, XNET_SCHEMAS,
} from '@xpell/xnet';
import { createXNetModule, XNET_OPS, XNET_SKILL } from '@xpell/xnet/server';
import { fixture, rejects } from './fixtures.mjs';

test('root/shared/client/server exports are explicit; transport is Node-only', async () => {
  const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
  assert.deepEqual(Object.keys(pkg.exports), ['.', './shared', './server', './client', './package.json']);
  assert.deepEqual(Object.keys(pkg.dependencies), ['better-sqlite3']);
  const shared = await import('@xpell/xnet/shared'); const client = await import('@xpell/xnet/client');
  assert.equal(shared.XNET_SCHEMAS, XNET_SCHEMAS); assert.equal(typeof client.XNetClient, 'function');
  assert.equal(typeof client.createXNetClient, 'function'); assert.equal(client.XNET_CLIENT_SKILL._runtime._target, 'server');
});

test('all V0.1 DTO validators accept persisted data; versions, shape and authority relations are checked', t => {
  const f = fixture(t); const a = f.store.accept(f.senders[0], f.intent()); const reg = f.register(); const d = f.dispatch(reg);
  assertXNetAddress(f.addresses[0]); assertXNetEndpointIdentity(f.endpoints[0]); assertXNetRegistration(reg);
  assertXNetBindingSource(f.sources[0]); assertXNetTrustedSource(d._envelope._source); assertXNetRouteBinding(f.routes[0]);
  assertXNetEnvelope(d._envelope); assertXNetSubmitIntent(f.intent()); assertXNetAcceptance(a);
  assertXNetDispatch(d); assertXNetAttempt(f.store.getStatus(f.senders[0], a._message_id)._attempts[0]); assertXNetDeliveryState('uncertain');
  const ack = f.ack(d); assertXNetProcessingAck(ack);
  assertXNetTargetReceipt({ _id: ack.receipt_id, _tenant_id: f.tenants[0], _endpoint_id: f.addresses[0]._endpoint_id,
    _message_id: a._message_id, _idempotency_key: 'event-1', _processed_at: f.now, _outcome: 'processed' });
  for (const change of [{ _envelope_version: 2 }, { _destination: f.addresses[1] }, { _expires_at: d._envelope._created_at },
    { _metadata: { authorization: 'private' } }, { _source: { ...d._envelope._source, _connection_id: 'spoofed' } }]) {
    rejects(assert, () => assertXNetEnvelope({ ...d._envelope, ...change }), 'E_XNET_INVALID_MESSAGE');
  }
  rejects(assert, () => assertXNetProcessingAck({ ...ack, receipt_id: undefined }), 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => assertXNetProcessingAck({ ...ack, error_code: 'TARGET_UNCERTAIN' }), 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => assertXNetEndpointIdentity({ ...f.endpoints[0], _id: randomUUID() }), 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => assertXNetRegistration({ ...reg, _lease_expires_at: reg._registered_at }), 'E_XNET_INVALID_MESSAGE');
});

test('malformed/non-JSON/deep/oversized payloads and authority fields cannot enter durable storage', t => {
  const f = fixture(t); const cycle = {}; cycle.self = cycle;
  const sparse = Array(1); sparse.extra = 1;
  const getter = Object.defineProperty({}, 'text', { enumerable: true, get() { throw new Error('must not execute'); } });
  const bad = [undefined, () => 1, new Date(), NaN, Infinity, 1n, Symbol('x'), cycle, sparse, getter,
    JSON.parse('{"__proto__":{"polluted":true}}'), Object.create({ text: 'inherited' }), { text: undefined }];
  let depth = null; for (let i = 0; i < 18; i++) depth = { nested: depth }; bad.push(depth);
  for (const payload of bad) rejects(assert, () => f.store.accept(f.senders[0], f.intent('bad', {
    payload: { ...f.intent().payload, _data: payload },
  })), 'E_XNET_INVALID_MESSAGE');
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('large', {
    payload: { ...f.intent().payload, _data: 'x'.repeat(65536) },
  })), 'E_XNET_TOO_LARGE');
  rejects(assert, () => f.store.accept(f.senders[0], f.intent('ref', { payload: { _mode: 'reference', _reference: '/private/path' } })), 'E_XNET_INVALID_MESSAGE');
  const literal = f.store.accept(f.senders[0], f.intent('literal', {
    payload: { ...f.intent().payload, _data: { tenant_id: f.tenants[1], destination: f.addresses[1], _module: 'other' } },
  }));
  assert.deepEqual(f.store.getStatus(f.senders[0], literal._message_id)._destination, f.addresses[0]);
});

test('XModule registers/discovers and uses only trusted local command attestation', async t => {
  const f = fixture(t); const authorities = new WeakMap();
  const module = createXNetModule({ _store: f.store, _authorize: c => authorities.get(c) });
  t.after(() => module.stop()); await _x.loadModuleAsync(module);
  assert.equal(module._name, 'xnet'); assert.equal(module.get_readiness()._ready, false);
  assert.equal(XNET_SKILL._runtime._target, 'server'); assert.equal(XNET_SKILL._ops, XNET_OPS);
  assert.equal(JSON.stringify(_x.getSkills()).includes('XNet durable core'), true);
  for (const [key, op] of Object.entries(XNET_OPS)) {
    assert.equal(op._id, key); assert.equal(op._name, key);
    for (const field of ['_input_schema', '_output_schema', '_permissions', '_errors', '_examples', '_side_effects', '_scope']) assert.ok(Object.hasOwn(op, field));
  }
  module.start(); assert.equal(module.get_readiness()._ready, true);
  async function call(principal, op, params, extra = {}) {
    const command = { _module: 'xnet', _op: op, _params: params, ...extra };
    if (principal) authorities.set(command, principal); return await _x.execute(command);
  }
  assert.equal((await call(undefined, 'submit', f.intent()))._ok, false);
  const spoof = await call(undefined, 'submit', { ...f.intent(), _auth: { _authenticated: true, ...f.senders[0] } });
  assert.equal(spoof._ok, false);
  const a = await call(f.senders[0], 'submit', f.intent()); assert.equal(a._ok, true);
  const reg = await call(f.receivers[0], 'register-endpoint', { address: f.addresses[0] }); assert.equal(reg._ok, true);
  const d = await call(f.receivers[0], 'receive-next', { registration_id: reg._result._id, request_key: 'module-test' }); assert.equal(d._ok, true);
  const ack = await call(f.receivers[0], 'ack-processed', f.ack(d._result)); assert.equal(ack._result._state, 'processed');
  const status = await call(f.senders[0], 'get-message-status', { message_id: a._result._message_id }); assert.equal(status._ok, true);
  assert.equal((await call(f.senders[1], 'get-message-status', { message_id: a._result._message_id }))._ok, false);
  assert.equal((await call(f.receivers[0], 'provision-endpoint', {}))._ok, false);
  assert.equal((await call(f.receivers[0], '_submit', f.intent()))._ok, false);
  assert.equal((await call(f.receivers[0], 'get-readiness', {}, { _object: 'bypass' }))._ok, false);
  assert.equal((await call(f.senders[0], 'submit', { ...f.intent(), _connection_id: 'spoofed' }))._ok, false);
  const key = `xnet:test:${randomUUID()}`; _xd.set(key, 'untouched', { source: 'xnet-test' });
  await assert.rejects(call(f.receivers[0], 'get-readiness', {}, { _output: { _target: 'xdata', _key: key } }), e => e._code === 'E_XNET_FORBIDDEN');
  assert.equal(_xd.get(key), 'untouched'); _xd.delete(key, { source: 'xnet-test' });
  const command = new XCommand({ _module: 'xnet', _op: 'get-readiness', _params: {} });
  authorities.set(command, f.senders[0]); assert.equal((await _x.execute(command))._ok, true);
});

test('missing config remains discoverable; explicit frame advancement is bounded and errors fail readiness', async t => {
  const missing = createXNetModule(); assert.deepEqual(missing.get_readiness()._missing_config, ['store', 'authorize']);
  rejects(assert, () => missing.start(), 'E_XNET_NOT_READY');
  const f = fixture(t, { _advance_limit: 1 });
  f.store.acceptBatch(f.senders[0], [f.intent('1', { ttl_ms: 1 }), f.intent('2', { ttl_ms: 1 })]);
  const module = createXNetModule({ _store: f.store, _authorize: () => f.senders[0], _advance_on_frame: true });
  module.start(); f.now += 2; assert.equal(module.advance(), 1); assert.equal(module.advance(), 1); assert.equal(module.advance(), 0);
  f.store.advance = () => { throw new Error('private storage path and secret'); };
  await module.onFrame(1); assert.equal(module.get_readiness()._ready, false);
  assert.equal(module.get_readiness()._error_code, 'E_XNET_STORE_UNAVAILABLE');
  const safe = await module.execute({ _module: 'xnet', _op: 'submit', _params: f.intent('3') });
  assert.equal(safe._ok, false); assert.equal(JSON.stringify(safe).includes('private'), false); module.stop();
});
