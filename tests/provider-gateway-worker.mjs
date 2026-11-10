// Isolated actual XNode actor. IPC is test control, never a provider/XNet transport.
import path from 'node:path';
import { mkdirSync } from 'node:fs';
import { timingSafeEqual, createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import Database from 'better-sqlite3';
import { XNode, WHWSClient, _x, _xd, wsGetConnections } from '@xpell/node';
import { SQLiteXNetStore, createXNetWormholesHost } from '@xpell/xnet/server';
import { SimulatedWhatsAppGateway } from './provider-fixtures/simulated-whatsapp.mjs';

let config, node, host, store, gateway, server, now, fault;
const probes = new Map(), revoked = new Set();
const drops = { acceptance: 0, available: 0, receive: 0, ack: 0 };
const dropped = { ...drops };
const code = e => e?._code ?? e?.code ?? 'MOCK_RUNTIME_FAILURE';
const hash = s => createHash('sha256').update(s).digest();
class Probe extends WHWSClient {
  async unauthenticated(command) { return (await this._callEnvelope('REQ', command, { _call_timeout_ms: 150 }))._payload; }
}
function installDrops() {
  for (const conn of wsGetConnections()) {
    if (conn._ws.__mock_drop) continue;
    conn._ws.__mock_drop = true;
    const send = conn._ws.send.bind(conn._ws);
    conn._ws.send = (data, ...args) => {
      const envelope = typeof data === 'string' ? JSON.parse(data) : undefined;
      const result = envelope?._payload?._result;
      const kind = envelope?._kind === 'EVT' && envelope?._payload?._name === 'xnet.available' ? 'available'
        : result?._stage === 'durably_accepted' ? 'acceptance' : result?._envelope ? 'receive'
          : result?._state === 'processed' ? 'ack' : undefined;
      if (kind && drops[kind] > 0) { drops[kind]--; dropped[kind]++; return; }
      return send(data, ...args);
    };
  }
}
async function probe(data) {
  let p = probes.get(data.id);
  if (!p) {
    p = new Probe({ _url: `ws://127.0.0.1:${server.address().port}/wh/v2`, _log: false,
      _call_timeout_ms: 150, _connect_timeout_ms: 500 });
    probes.set(data.id, p); await p.connect();
  }
  if (data.auth !== undefined) return p.auth({ _token: data.auth });
  installDrops();
  return data.unauth ? p.unauthenticated(data.command) : p.call(data.command);
}
async function stop() {
  gateway?.stop();
  for (const p of probes.values()) p.close(); probes.clear();
  await host?.stop();
  const first = node?.stop(), second = node?.stop(); await first;
  const frame = _xd.get('engine:frame-number'); await delay(20);
  return { sameStop: first === second, frameStable: frame === _xd.get('engine:frame-number'),
    coreRunning: _x.isRunning(), listening: server?.listening ?? false,
    connections: wsGetConnections().length, upgradeListeners: server?.listenerCount('upgrade') ?? 0 };
}
async function execute(op, data) {
  if (data.now !== undefined) now = data.now;
  if (op === 'initialize') {
    config = data; now = data.now;
    // Validate host-owned mapping before opening the runtime or connecting any producer.
    const gatewayConfig = { ...data.gateway, _provisioning: data.provisioning, _credentials: data.credentials };
    gateway = new SimulatedWhatsAppGateway({ ...gatewayConfig, _url: 'ws://127.0.0.1:1/wh/v2' });
    try {
      store = new SQLiteXNetStore({ _db_path: data.db, _clock: () => now, _claim_key: Buffer.alloc(32, 42),
        _limits: data.limits, _fault: point => { if (point === fault) throw new Error('synthetic rollback'); } });
      store.open();
      host = createXNetWormholesHost({ _store: store, _clock: () => now, _hint_interval_ms: 1,
        _verify_credential: token => {
          const c = config.credentials.find(c => timingSafeEqual(hash(c._token), hash(token)));
          return c && !revoked.has(c._id) && c._id !== config.reject_credential
            ? { _principal: c._id === config.mismatch_credential ? config.credentials[1]._principal : c._principal,
              _expires_at: 2_000_000 } : undefined;
        } });
      node = new XNode();
      const work = path.join(data.dir, 'node-work'), systems = path.join(data.dir, 'empty-system-apps');
      mkdirSync(systems, { recursive: true });
      const hostOptions = host.nodeOptions();
      await node.start({ ...hostOptions, _host: '127.0.0.1', _port: 0,
        _work_folder: work, _system_xapps_path: systems,
        _web_settings: { ...hostOptions._web_settings, 'enable-ssl': false, domain: 'public.example.invalid' },
        _xdb: { _type: 'sqlite', _root: path.join(work, 'xdb'), _cache: false } });
      server = node._web_server._web_server;
      await host.start(server);
      gateway = new SimulatedWhatsAppGateway({ ...gatewayConfig, _url: `ws://127.0.0.1:${server.address().port}/wh/v2` });
      await gateway.start(); installDrops();
      return { address: server.address(), loaded: _x.getModule('xnet') === host._module,
        ready: host._module.get_readiness(), connections: wsGetConnections().length,
        upgrades: server.listenerCount('upgrade') };
    } catch (error) { await stop(); throw error; }
  }
  if (op === 'ingest') return gateway.ingest({ ...data.input,
    ...(data.input?._raw === undefined ? {} : { _raw: Buffer.from(data.input._raw, 'base64') }) });
  if (op === 'concurrent-ingest') return Promise.all(data.inputs.map(input => gateway.ingest({ ...input, _raw: Buffer.from(input._raw, 'base64') })));
  if (op === 'probe') return probe(data);
  if (op === 'advance') { installDrops(); await Promise.all([host.advance(), host.advance()]); return {}; }
  if (op === 'drop') { Object.assign(drops, data); installDrops(); return {}; }
  if (op === 'fault') { fault = data.point; return {}; }
  if (op === 'revoke') { revoked.add(data.ref); return {}; }
  if (op === 'inspect') {
    const db = new Database(config.db, { readonly: true });
    try { return { messages: db.prepare('SELECT id,tenant_id,endpoint_id,envelope FROM xnet_message').all(),
      deliveries: db.prepare('SELECT message_id,state,attempt_count,receipt_id FROM xnet_delivery').all(),
      attempts: db.prepare('SELECT id,outcome FROM xnet_attempt').all(), acks: db.prepare('SELECT receipt_id,outcome FROM xnet_ack').all(),
      registrations: db.prepare('SELECT principal_id FROM xnet_registration WHERE active=1').all(), dropped }; }
    finally { db.close(); }
  }
  // Negative-test admin edits only the disposable fixture store, never production data/API.
  if (op === 'disable') {
    const table = { account: 'xnet_account', endpoint: 'xnet_endpoint', grant: 'xnet_grant', route: 'xnet_route' }[data.kind];
    if (!table) throw new Error('invalid test table');
    const idColumn = data.kind === 'grant' ? 'principal_id' : 'id';
    const db = new Database(config.db);
    try {
      const row = db.prepare(`SELECT definition FROM ${table} WHERE ${idColumn}=?`).get(data.id);
      const definition = { ...JSON.parse(row.definition), _enabled: data.enabled ?? false };
      db.prepare(`UPDATE ${table} SET definition=? WHERE ${idColumn}=?`).run(JSON.stringify(definition), data.id);
    } finally { db.close(); }
    return {};
  }
  if (op === 'stop-gateway') { gateway.stop(); return {}; }
  if (op === 'stop') return stop();
  throw new Error('unknown fixture operation');
}
process.on('message', async message => {
  try {
    const result = await execute(message.op, message.data ?? {});
    process.send({ id: message.id, result }, () => { if (message.op === 'stop') process.disconnect(); });
  } catch (error) { process.send({ id: message.id, error: code(error) }); }
});
