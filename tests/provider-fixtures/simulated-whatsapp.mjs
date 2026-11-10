// Private simulation only. No Meta HTTP route, provider SDK or Agent/Channel dependency.
import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { WHWSClient } from '@xpell/node';
import { assertXNetJson, assertXNetSubmitIntent, assertXNetAcceptance } from '@xpell/xnet/shared';

export const TYPE = 'mock.inbound-text.v1';
const fail = code => { throw Object.assign(new Error('Simulated provider gateway rejected input'), { _code: code }); };
const bad = () => fail('MOCK_INVALID_EVENT');
const phone = value => typeof value === 'string' && /^[0-9]{1,32}$/u.test(value);
const external = value => typeof value === 'string' && /^[A-Za-z0-9._~:/+=-]{1,512}$/u.test(value);
const samePrincipal = (a, b) => a?._principal_id === b?._principal_id && a?._tenant_id === b?._tenant_id;
function closed(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || required.some(k => !Object.hasOwn(value, k))
    || Object.keys(value).some(k => !required.includes(k) && !optional.includes(k))) bad();
}
function one(value) { if (!Array.isArray(value) || value.length !== 1) bad(); return value[0]; }

// Construction-time configuration is host authority, never provider-supplied input.
export function validateBindings(input) {
  const c = structuredClone(input);
  const reject = () => fail('MOCK_INVALID_BINDING');
  if (!c._profile?._id || typeof c._profile._secret !== 'string' || c._profile._secret.length < 32
    || !Array.isArray(c._bindings) || c._bindings.length !== 2) reject();
  const phones = new Set(), principals = new Set(), refs = new Set();
  const p = c._provisioning;
  if (new Set(c._credentials.map(k => k._token)).size !== c._credentials.length) reject();
  for (const b of c._bindings) {
    if (b._profile_id !== c._profile._id || !phone(b._receiving_phone_id) || !phone(b._waba_id)
      || phones.has(b._receiving_phone_id) || principals.has(b._principal_id) || refs.has(b._credential_ref)) reject();
    phones.add(b._receiving_phone_id); principals.add(b._principal_id); refs.add(b._credential_ref);
    const accounts = p.accounts.filter(a => a._id === b._account_binding_id);
    const grants = p.grants.filter(g => g._principal_id === b._principal_id);
    const credentials = c._credentials.filter(k => k._id === b._credential_ref);
    if (accounts.length !== 1 || grants.length !== 1 || credentials.length !== 1) reject();
    const a = accounts[0], g = grants[0], credential = credentials[0];
    if (!a._enabled || a._provider !== 'whatsapp' || a._adapter_id !== 'sim-whatsapp'
      || a._account_locator !== b._receiving_phone_id || a._tenant_id !== g._tenant_id
      || !p.tenants.includes(a._tenant_id) || !g._enabled || !samePrincipal(g, credential._principal)
      || typeof credential._token !== 'string' || credential._token.length < 32
      || g._source?._kind !== 'provider_account' || g._source._adapter_id !== a._adapter_id
      || g._source._account_binding_id !== a._id || g._receive_endpoint_ids.length !== 0
      || !g._permissions.includes('submit') || g._permissions.some(op => !['submit', 'status'].includes(op))) reject();
    const matches = p.accounts.filter(other => other._provider === 'whatsapp' && other._account_locator === a._account_locator);
    const routes = p.routes.filter(r => r._tenant_id === a._tenant_id && r._source._kind === 'provider_account'
      && r._source._adapter_id === a._adapter_id && r._source._account_binding_id === a._id && r._message_type === TYPE);
    if (matches.length !== 1 || routes.length !== 1 || !routes[0]._enabled) reject();
    const address = routes[0]._destination;
    const endpoint = p.endpoints.find(e => e._id === address._endpoint_id && e._address._tenant_id === a._tenant_id);
    const receivers = p.grants.filter(g => g._tenant_id === a._tenant_id && g._receive_endpoint_ids.includes(address._endpoint_id));
    if (address._tenant_id !== a._tenant_id || !endpoint?._enabled || !endpoint._accepted_message_types.includes(TYPE)
      || receivers.length !== 1 || !receivers[0]._enabled || receivers[0]._source
      || receivers[0]._receive_endpoint_ids.length !== 1
      || ['receive', 'ack', 'status'].some(op => !receivers[0]._permissions.includes(op))
      || receivers[0]._permissions.some(op => !['receive', 'ack', 'status'].includes(op))) reject();
  }
  // The complete copied configuration stays private; callers cannot change bindings after validation.
  return c;
}

function normalize(raw, signature, profile, bindings) {
  if (!Buffer.isBuffer(raw) || raw.length > 256 * 1024 || typeof signature !== 'string'
    || !/^sha256=[0-9a-f]{64}$/u.test(signature)) fail('MOCK_BAD_SIGNATURE');
  const expected = createHmac('sha256', profile._secret).update(raw).digest();
  if (!timingSafeEqual(expected, Buffer.from(signature.slice(7), 'hex'))) fail('MOCK_BAD_SIGNATURE');
  let value;
  try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(raw)); assertXNetJson(value); } catch { bad(); }
  closed(value, ['object', 'entry']);
  if (value.object !== 'whatsapp_business_account') bad();
  const entry = one(value.entry); closed(entry, ['id', 'changes']);
  const change = one(entry.changes); closed(change, ['field', 'value']);
  if (change.field !== 'messages') bad();
  closed(change.value, ['metadata', 'messages']);
  closed(change.value.metadata, ['phone_number_id']);
  const locator = change.value.metadata.phone_number_id;
  if (!phone(locator) || !phone(entry.id)) bad();
  const binding = bindings.find(b => b._receiving_phone_id === locator && b._waba_id === entry.id);
  if (!binding) fail('MOCK_UNBOUND_ACCOUNT');
  const message = one(change.value.messages);
  closed(message, ['id', 'from', 'type', 'text'], ['timestamp', 'context']);
  closed(message.text, ['body']);
  if (message.type !== 'text' || !external(message.id) || !phone(message.from)
    || typeof message.text.body !== 'string' || !message.text.body.length || message.text.body.length > 4096) bad();
  const data = { _receiving_phone_id: locator, _waba_id: entry.id, _external_user_id: message.from,
    _external_conversation_id: message.from, _external_message_id: message.id, _text: message.text.body };
  if (message.timestamp !== undefined) {
    if (typeof message.timestamp !== 'string' || !/^[0-9]{1,12}$/u.test(message.timestamp)) bad();
    data._provider_timestamp_ms = Number(message.timestamp) * 1000;
    if (!Number.isSafeInteger(data._provider_timestamp_ms)) bad();
  }
  if (message.context !== undefined) {
    closed(message.context, ['id']); if (!external(message.context.id)) bad();
    data._reply_to_external_message_id = message.context.id;
  }
  const key = 'wa:v1:' + createHash('sha256').update(JSON.stringify([locator, message.id]), 'utf8').digest('hex');
  const intent = { message_type: TYPE, event_id: key, idempotency_key: key,
    payload: { _mode: 'inline', _content_type: 'application/json', _schema_id: 'mock.whatsapp.inbound.v1', _data: data } };
  assertXNetSubmitIntent(intent);
  return { binding, intent };
}

export class SimulatedWhatsAppGateway {
  #config; #clients = new Map(); #ready = false;
  constructor(config) { this.#config = validateBindings(config); }
  async start() {
    if (this.#ready) return;
    try {
      for (const b of this.#config._bindings) {
        const credential = this.#config._credentials.find(c => c._id === b._credential_ref);
        const client = new WHWSClient({ _url: this.#config._url, _log: false, _auto_reconnect: false,
          _call_timeout_ms: 150, _connect_timeout_ms: 500 });
        this.#clients.set(b._receiving_phone_id, client);
        const auth = await client.auth({ _token: credential._token });
        if (!auth._ok || auth._result?._user_id !== b._principal_id) fail('MOCK_AUTH_FAILED');
      }
      this.#ready = true;
    } catch (error) { this.stop(); throw error; }
  }
  async ingest(input) {
    if (!this.#ready) fail('MOCK_NOT_READY');
    closed(input, ['_raw', '_signature']);
    const { binding, intent } = normalize(input._raw, input._signature, this.#config._profile, this.#config._bindings);
    const response = await this.#clients.get(binding._receiving_phone_id).call({ _module: 'xnet', _op: 'submit', _params: intent });
    assertXNetAcceptance(response._result);
    return response._result; // Durable acceptance only; never reports business processing.
  }
  stop() { this.#ready = false; for (const client of this.#clients.values()) client.close(); this.#clients.clear(); }
}
