import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { SQLiteXNetStore } from '@xpell/xnet/server';

export const KEY = Buffer.alloc(32, 42);
export const TYPE = 'mock.inbound-text.v1';
export function fixture(t, overrides = {}, acceptedMessageTypes = [TYPE]) {
  const dir = mkdtempSync(path.join(tmpdir(), 'xnet-1a-'));
  const f = { dir, now: 1_000_000, fault: undefined, instance_id: randomUUID(), stores: [] };
  f.options = { _db_path: path.join(dir, 'mailbox.sqlite'), _claim_key: KEY, _instance_id: f.instance_id,
    _clock: () => f.now, _fault: point => f.fault?.(point),
    _limits: { _ttl_ms: 1000, _claim_lease_ms: 50, _registration_lease_ms: 10_000,
      _retry_base_ms: 10, _retry_max_ms: 20, _max_attempts: 3, ...overrides } };
  f.store = new SQLiteXNetStore(f.options); f.store.initialize(); f.stores.push(f.store);
  f.tenants = [randomUUID(), randomUUID()];
  f.addresses = f.tenants.map(_tenant_id => ({ _tenant_id, _endpoint_id: randomUUID() }));
  f.endpoints = f.addresses.map(_address => ({ _id: _address._endpoint_id, _address, _kind: 'agent',
    _name: 'sim-agent', _enabled: true, _accepted_message_types: acceptedMessageTypes, _processing_contract: 'idempotent-v1' }));
  f.accounts = f.tenants.map((_tenant_id, i) => ({ _id: randomUUID(), _tenant_id, _adapter_id: 'mock-ingress',
    _provider: 'mock', _account_locator: `10000000000000${i + 1}`, _enabled: true }));
  f.senders = f.tenants.map(_tenant_id => ({ _principal_id: randomUUID(), _tenant_id }));
  f.receivers = f.tenants.map(_tenant_id => ({ _principal_id: randomUUID(), _tenant_id }));
  f.sources = f.accounts.map(a => ({ _kind: 'provider_account', _adapter_id: a._adapter_id, _account_binding_id: a._id }));
  f.routes = f.tenants.map((_tenant_id, i) => ({ _id: randomUUID(), _tenant_id, _version: 1, _source: f.sources[i],
    _message_type: TYPE, _destination: f.addresses[i], _enabled: true, _updated_at: f.now, _updated_by: randomUUID() }));
  f.provisioning = { tenants: f.tenants, endpoints: f.endpoints, accounts: f.accounts,
    grants: f.tenants.flatMap((_tenant_id, i) => [
      { ...f.senders[i], _enabled: true, _permissions: ['submit', 'status'], _source: f.sources[i], _receive_endpoint_ids: [] },
      { ...f.receivers[i], _enabled: true, _permissions: ['submit', 'receive', 'ack', 'status'],
        _source: { _kind: 'endpoint', _address: f.addresses[i] }, _receive_endpoint_ids: [f.addresses[i]._endpoint_id] },
    ]), routes: f.routes };
  f.store.provision(f.provisioning);
  f.intent = (id = 'event-1', extra = {}) => ({ message_type: TYPE, idempotency_key: id, event_id: id,
    payload: { _mode: 'inline', _content_type: 'application/json', _schema_id: 'mock.text.v1', _data: { text: 'simulated message' } }, ...extra });
  f.register = (i = 0) => f.store.registerEndpoint(f.receivers[i], f.addresses[i]);
  f.claim = (reg, key = randomUUID(), i = 0) => f.store.claimNext(f.receivers[i], reg._id, key);
  f.dispatch = (reg, key = randomUUID(), i = 0) => {
    const d = f.claim(reg, key, i); if (d) f.store.markDispatched(f.receivers[i], d); return d;
  };
  f.ack = (d, extra = {}) => Object.fromEntries(Object.entries({ delivery_id: d._delivery_id, message_id: d._envelope._message_id,
    attempt_id: d._attempt_id, registration_id: d._registration_id, owner_epoch: d._owner_epoch,
    claim_token: d._claim_token, outcome: 'processed', receipt_id: randomUUID(), ...extra }).filter(([, value]) => value !== undefined));
  t.after(() => { for (const s of f.stores) s.close(); rmSync(dir, { recursive: true, force: true }); });
  return f;
}
export const rejects = (assert, fn, code) => assert.throws(fn, e => e._code === code);
