// Isolated test runtime. IPC is fixture control, never part of the XNet protocol.
import { createServer } from 'node:http';
import { createHash, randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import { _x, XModule, XResponseOK } from '@xpell/core';
import { WHWSClient, wsGetConnections } from '@xpell/node';
import { SQLiteXNetStore, createXNetWormholesHost } from '@xpell/xnet/server';
import { createXNetClient } from '@xpell/xnet/client';

let now = 1_000_000;
let config, store, host, server, client, ledger;
let calls = 0;
let advancing;
const drops = { available: 0, receive: 0, ack: 0 };
const dropped = { available: 0, receive: 0, ack: 0 };
const dropWaiters = new Map();
const probes = new Map();
const hash = token => createHash('sha256').update(token).digest('hex');
const code = e => typeof e?._code === 'string' ? e._code : typeof e?.code === 'string' ? e.code : 'TEST_RUNTIME_ERROR';
class Probe extends WHWSClient {
  // Adversarial fixture uses the existing codec/session to send a real unauthenticated REQ.
  async requestWithoutAuth(command) { return (await this._callEnvelope('REQ', command, { _call_timeout_ms: 1000 }))._payload; }
}
async function initialize(input) {
  config = input;
  now = input.now;
  if (input.role === 'server') {
    store = new SQLiteXNetStore({ _db_path: input.db, _claim_key: Buffer.alloc(32, 42), _clock: () => now, _limits: input.limits }); store.open();
    host = createXNetWormholesHost({ _store: store, _clock: () => now, _hint_interval_ms: 20, _frame_limit: input.frame_limit ?? 20,
      _verify_credential: token => {
        const index = input.tokens.findIndex(t => hash(token) === hash(t));
        return index >= 0 && !input.revoked?.includes(index)
          ? { _principal: input.receivers[index], _expires_at: input.auth_expires_at ?? 2_000_000 } : undefined;
      } });
    // Test-owned HTTP fixture: exact loopback binding, no XNode service/engine startup.
    // Production composition uses host.nodeOptions() plus the existing XNode server.
    server = createServer((_req, res) => { res.writeHead(404); res.end(); });
    await new Promise((resolve, reject) => { server.once('error', reject); server.listen(input.port ?? 0, '127.0.0.1', resolve); });
    await host.start(server);
    // Failure injection only: suppress already-serialized canonical transport frames.
    // The public registry exposes server connections; intercept their send after creation.
    // Install this from the server's upgrade event on the next turn, before fixture RPC calls.
    const installDrops = () => {
      for (const conn of wsGetConnections()) {
        if (conn._ws.__xnet_test_drop) continue;
        conn._ws.__xnet_test_drop = true;
        const original = conn._ws.send.bind(conn._ws);
        conn._ws.send = (data, ...args) => {
          const envelope = typeof data === 'string' ? JSON.parse(data) : undefined;
          const result = envelope?._payload?._result;
          const kind = envelope?._kind === 'EVT' && envelope?._payload?._name === 'xnet.available' ? 'available'
            : result?._envelope ? 'receive' : result?._state === 'processed' ? 'ack' : undefined;
          if (kind && drops[kind] > 0) {
            drops[kind]--; dropped[kind]++;
            dropWaiters.get(kind)?.(); dropWaiters.delete(kind); return;
          }
          return original(data, ...args);
        };
      }
    };
    config.installDrops = installDrops;
    server.on('upgrade', installDrops);
    return { port: server.address().port };
  }
  ledger = new Database(input.ledger);
  ledger.pragma('journal_mode=WAL'); ledger.pragma('synchronous=FULL');
  ledger.exec(`CREATE TABLE IF NOT EXISTS receipt(tenant TEXT,endpoint TEXT,message TEXT,id TEXT,idem TEXT,processed_at INTEGER,PRIMARY KEY(tenant,endpoint,message));
    CREATE TABLE IF NOT EXISTS effect(message TEXT PRIMARY KEY,tenant TEXT,endpoint TEXT);`);
  const processTarget = ledger.transaction(envelope => {
    if (envelope._tenant_id !== input.address._tenant_id || envelope._destination._endpoint_id !== input.address._endpoint_id) throw new Error('wrong target');
    const old = ledger.prepare('SELECT * FROM receipt WHERE tenant=? AND endpoint=? AND message=?').get(envelope._tenant_id, input.address._endpoint_id, envelope._message_id);
    if (old) {
      if (old.idem !== envelope._idempotency_key) throw new Error('intent conflict');
      return old;
    }
    const row = { tenant: envelope._tenant_id, endpoint: input.address._endpoint_id, message: envelope._message_id,
      id: randomUUID(), idem: envelope._idempotency_key, processed_at: now };
    ledger.prepare('INSERT INTO effect VALUES (?,?,?)').run(row.message, row.tenant, row.endpoint);
    ledger.prepare('INSERT INTO receipt VALUES (?,?,?,?,?,?)').run(row.tenant, row.endpoint, row.message, row.id, row.idem, row.processed_at);
    return row;
  }).immediate;
  class Agent extends XModule {
    static _name = 'sim-agent';
    constructor() { super({ _name: 'sim-agent' }); }
    async _process_message(command) {
      calls++;
      if (config.uncertain) return new XResponseOK({ _outcome: 'uncertain' }).toXData();
      const row = processTarget(command._params.envelope);
      return new XResponseOK({ _id: row.id, _tenant_id: config.foreign_tenant ?? row.tenant, _endpoint_id: row.endpoint,
        _message_id: row.message, _idempotency_key: row.idem, _processed_at: row.processed_at, _outcome: 'processed' }).toXData();
    }
  }
  await _x.loadModuleAsync(new Agent());
  client = createXNetClient({ _url: input.url, _address: input.address, _credential: () => input.token,
    _handlers: input.no_handler ? {} : { 'mock.inbound-text.v1': { _module: 'sim-agent', _op: 'process-message' } }, _clock: () => now,
    _max_drain: 1, _call_timeout_ms: 40, _connect_timeout_ms: 300, _reconnect_base_ms: 10, _reconnect_max_ms: 100 });
  await _x.loadModuleAsync(client); await client.start();
  return { readiness: client.get_readiness(), registration: client.get_registration() };
}
async function execute(op, data) {
  if (data?.now !== undefined) now = data.now;
  if (op === 'initialize') return initialize(data);
  if (config.role === 'server') {
    if (op === 'advance') { config.installDrops(); await host.advance(); return {}; }
    if (op === 'drop') { Object.assign(drops, data); config.installDrops(); return {}; }
    if (op === 'wait-drop') {
      if (dropped[data.kind]) return {};
      return new Promise(resolve => { dropWaiters.set(data.kind, () => resolve({})); });
    }
    if (op === 'accept') {
      // Mock verification and locator binding are fixture host authority; payload fields never choose a tenant.
      if (data.signature !== 'mock-verified') throw { _code: 'MOCK_BAD_SIGNATURE' };
      const i = config.phone_ids.indexOf(data.phone_id); if (i < 0) throw { _code: 'MOCK_UNBOUND_ACCOUNT' };
      return store.accept(config.senders[i], { message_type: data.message_type ?? 'mock.inbound-text.v1', event_id: data.event,
        idempotency_key: data.event, ...(data.ttl_ms ? { ttl_ms: data.ttl_ms } : {}),
        payload: { _mode: 'inline', _content_type: 'application/json', _schema_id: 'mock.text.v1', _data: data.payload ?? { text: 'mock' } } });
    }
    if (op === 'status') return store.getStatus(config.senders[data.tenant ?? 0], data.message);
    if (op === 'disconnect') {
      for (const c of wsGetConnections()) if (c._ctx._auth._user_id === config.receivers[data.tenant]._principal_id) c._ws.terminate();
      return {};
    }
    if (op === 'revoke') {
      if (data.credential) (config.revoked ??= []).push(data.tenant);
      if (data.grant) {
        const db = new Database(config.db); const p = config.receivers[data.tenant];
        const row = db.prepare('SELECT definition FROM xnet_grant WHERE tenant_id=? AND principal_id=?').get(p._tenant_id, p._principal_id);
        db.prepare('UPDATE xnet_grant SET definition=? WHERE tenant_id=? AND principal_id=?').run(JSON.stringify({ ...JSON.parse(row.definition), _enabled: false }), p._tenant_id, p._principal_id); db.close();
      }
      return {};
    }
    if (op === 'probe') {
      let probe = probes.get(data.id);
      if (!probe) { probe = new Probe({ _url: `ws://127.0.0.1:${server.address().port}/wh/v2`, _log: false, _call_timeout_ms: 1000 }); probes.set(data.id, probe); await probe.connect(); }
      if (data.auth !== undefined) return probe.auth({ _token: data.auth });
      config.installDrops();
      if (data.close) { probe.close(); probes.delete(data.id); return {}; }
      return data.unauth ? probe.requestWithoutAuth(data.command) : probe.call(data.command);
    }
    if (op === 'stop') {
      for (const p of probes.values()) p.close(); probes.clear(); await host.stop();
      await new Promise(resolve => server.close(resolve)); return {};
    }
  } else {
    if (op === 'advance') { advancing = client.advance(); await advancing; return { readiness: client.get_readiness(), registration: client.get_registration() }; }
    if (op === 'inspect') return { readiness: client.get_readiness(), registration: client.get_registration(), calls,
      effects: ledger.prepare('SELECT * FROM effect').all(), receipts: ledger.prepare('SELECT * FROM receipt').all() };
    if (op === 'uncertain') { config.uncertain = true; return {}; }
    if (op === 'stop') { client.stop(); await advancing; ledger.close(); return {}; }
  }
  throw { _code: 'UNKNOWN_TEST_OPERATION' };
}
process.on('message', async message => {
  try {
    const result = await execute(message.op, message.data);
    if (process.connected) process.send({ id: message.id, result });
    if (message.op === 'stop') process.disconnect();
  } catch (e) { if (process.connected) process.send({ id: message.id, error: code(e) }); }
});
