import { _x, _xu, XModule, XResponseOK, type XCommandData, type XResponseData, type XpellSkill } from "@xpell/core";
import { WHWSClient } from "@xpell/node";
import { assertXNetAddress, assertXNetContract, assertXNetDispatch, assertXNetRegistration, assertXNetTargetReceipt,
  assertXNetJson, XNET_AVAILABLE_EVENT, XNetError, xnetReject } from "../shared/index.js";
import type { XNetAddress, XNetDispatch, XNetEndpointRegistration, XNetProcessingAck, XNetTargetReceipt } from "../shared/index.js";

export type XNetLocalHandlerResult = XNetTargetReceipt | { readonly _outcome: "retryable_failure" | "permanent_failure" | "uncertain" };

export interface XNetClientOptions {
  readonly _url: string;
  readonly _address: XNetAddress;
  readonly _credential: () => string | Promise<string>;
  /** Message type -> fixed local Core operation. Payloads cannot choose commands. */
  readonly _handlers: Readonly<Record<string, { readonly _module: string; readonly _op: string }>>;
  readonly _execute?: (command: XCommandData) => unknown | Promise<unknown>;
  readonly _clock?: () => number;
  readonly _max_drain?: number;
  readonly _max_reconnect_attempts?: number;
  readonly _reconnect_base_ms?: number;
  readonly _reconnect_max_ms?: number;
  readonly _call_timeout_ms?: number;
  readonly _connect_timeout_ms?: number;
}
export const XNET_CLIENT_OPS = {
  "get-readiness": { _id: "get-readiness", _name: "get-readiness", _scope: "server", _description: "Read safe Node endpoint readiness.",
    _input_schema: { type: "object", properties: {}, additionalProperties: false }, _output_schema: { type: "object" },
    _permissions: ["xnet-client:get-readiness"], _errors: [], _examples: [], _side_effects: "none" },
};
export const XNET_CLIENT_SKILL: XpellSkill & Record<string, unknown> = { _id: "xnet-client", _name: "xnet-client", _title: "XNet Node endpoint", _package: "@xpell/xnet", _version: "0.2.0",
  _scope: "server", _type: "server-module-api", _runtime: { _target: "server", _hosts: ["node"] },
  _description: "Outbound Node Wormholes endpoint; explicitly driven lifecycle, allowlisted local handlers.", _ops: XNET_CLIENT_OPS,
  _config: [{ _name: "url", _required: true }, { _name: "address", _required: true }, { _name: "handlers", _required: true }],
  _secrets: [{ _name: "credential", _owner: "host" }], _permissions: ["xnet-client:get-readiness"],
  _readiness: { _checks: ["explicit_start", "authentication", "registration"] }, _examples: [],
  _exports: { _modules: [{ _name: "xnet-client", _scope: "server", _ops: Object.values(XNET_CLIENT_OPS) }] } };

/** Node-only XModule. onFrame/advance drives all XNet work; EVT merely marks a hint. */
export class XNetClient extends XModule {
  static _name = "xnet-client"; static _ops = XNET_CLIENT_OPS; static _skill = XNET_CLIENT_SKILL;
  private readonly options: XNetClientOptions;
  private wh?: WHWSClient;
  private registration?: XNetEndpointRegistration;
  private authenticatedPrincipal?: string;
  private authenticatedSession?: string;
  private receiveKey?: string;
  private pendingAck?: XNetProcessingAck;
  private busy = false;
  private running = false;
  private dirty = true;
  private state = "stopped";
  private reconnects = 0;
  private rpcFailures = 0;
  private nextConnect = 0;
  private renewAt = 0;
  private lastError?: string;
  private cancel?: () => void;
  private cancelled: Promise<never> = new Promise(() => {});
  private generation = 0;

  constructor(options: XNetClientOptions) {
    super({ _name: "xnet-client" });
    assertXNetAddress(options._address); assertXNetJson(options._handlers);
    const url = new URL(options._url);
    if (!["ws:", "wss:"].includes(url.protocol) || url.username || url.password || url.search || url.hash
      || typeof options._credential !== "function") xnetReject("E_XNET_INVALID_CONFIG");
    for (const handler of Object.values(options._handlers)) {
      if (!handler || Object.keys(handler).some(k => !["_module", "_op"].includes(k))
        || !/^[a-z][a-z0-9-]*$/u.test(handler._module) || !/^[a-z][a-z0-9-]*$/u.test(handler._op)) xnetReject("E_XNET_INVALID_CONFIG");
    }
    this.options = { _max_drain: 4, _max_reconnect_attempts: 8, _reconnect_base_ms: 1000, _reconnect_max_ms: 60_000,
      _call_timeout_ms: 10_000, _connect_timeout_ms: 10_000, ...options,
      _address: { ...options._address }, _handlers: JSON.parse(JSON.stringify(options._handlers)) };
    for (const key of ["_max_drain", "_max_reconnect_attempts", "_reconnect_base_ms", "_reconnect_max_ms", "_call_timeout_ms", "_connect_timeout_ms"] as const) {
      const n = this.options[key]; if (!Number.isSafeInteger(n) || n! <= 0) xnetReject("E_XNET_INVALID_CONFIG");
    }
    if (this.options._max_drain! > 100 || this.options._max_reconnect_attempts! > 100
      || this.options._call_timeout_ms! > 30_000 || this.options._connect_timeout_ms! > 30_000) xnetReject("E_XNET_INVALID_CONFIG");
  }
  private now(): number {
    const now = (this.options._clock ?? Date.now)();
    if (!Number.isSafeInteger(now) || now < 0) xnetReject("E_XNET_INVALID_CLOCK"); return now;
  }
  get_readiness() {
    return { _ready: this.running && !!this.registration && this.registration._lease_expires_at > this.now() && this.wh?.isOpen() === true,
      _status: this.state, _reconnect_attempts: this.reconnects, ...(this.lastError ? { _error_code: this.lastError } : {}) };
  }
  get_registration(): XNetEndpointRegistration | undefined { return this.registration && JSON.parse(JSON.stringify(this.registration)); }
  async _get_readiness() { return new XResponseOK(this.get_readiness()).toXData(); }
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true; this.generation++; this.reconnects = 0; this.nextConnect = 0; this.dirty = true;
    this.cancelled = new Promise<never>((_, reject) => { this.cancel = () => reject(new XNetError("E_XNET_STOPPED")); });
    void this.cancelled.catch(() => {});
    await this.advance();
  }
  stop(): void {
    this.running = false; this.generation++; this.cancel?.(); this.cancel = undefined;
    this.wh?.close(); this.wh = undefined; this.registration = undefined;
    this.authenticatedPrincipal = undefined; this.authenticatedSession = undefined;
    this.pendingAck = undefined; this.receiveKey = undefined; this.state = "stopped";
  }
  override async onFrame(frame_number: number): Promise<void> { await super.onFrame(frame_number); await this.advance(); }
  private wait<T>(promise: Promise<T>): Promise<T> { return Promise.race([promise, this.cancelled]); }
  private async call(op: string, params: Record<string, unknown>): Promise<any> {
    if (!this.wh?.isOpen()) throw new XNetError("E_XNET_TRANSPORT");
    const result = await this.wait(this.wh.call({ _module: "xnet", _op: op, _params: params }));
    return result._result;
  }
  private setRegistration(value: unknown): void {
    assertXNetRegistration(value);
    if (value._endpoint._tenant_id !== this.options._address._tenant_id || value._endpoint._endpoint_id !== this.options._address._endpoint_id
      || !value._connection_id || value._session_id !== this.authenticatedSession
      || value._principal_id !== this.authenticatedPrincipal || value._lease_expires_at <= this.now()) xnetReject("E_XNET_FORBIDDEN");
    this.registration = value;
    this.renewAt = this.now() + Math.max(1, Math.floor((value._lease_expires_at - this.now()) / 3));
  }
  private reconnect(): void {
    this.wh?.close(); this.wh = undefined; this.registration = undefined;
    this.authenticatedPrincipal = undefined; this.authenticatedSession = undefined;
    this.receiveKey = undefined; this.pendingAck = undefined; this.dirty = true; this.rpcFailures = 0;
    this.reconnects++;
    if (this.reconnects >= this.options._max_reconnect_attempts!) { this.running = false; this.state = "failed"; return; }
    this.nextConnect = this.now() + Math.min(this.options._reconnect_max_ms!, this.options._reconnect_base_ms! * 2 ** Math.min(this.reconnects - 1, 30));
    this.state = "reconnect_wait";
  }
  async advance(): Promise<void> {
    if (!this.running || this.busy) return;
    this.busy = true; const generation = this.generation;
    try {
      if (!this.wh?.isOpen()) {
        if (this.wh) { this.reconnect(); return; }
        if (this.now() < this.nextConnect) return;
        this.state = "connecting";
        const wh = new WHWSClient({ _url: this.options._url, _auto_reconnect: false, _log: false,
          _call_timeout_ms: this.options._call_timeout_ms, _connect_timeout_ms: this.options._connect_timeout_ms });
        this.wh = wh;
        wh.onEvt(evt => {
          const hint = evt?._args?.[0];
          if (evt?._name !== XNET_AVAILABLE_EVENT) return;
          try {
            assertXNetContract("available", hint);
            if (hint?._registration_id === this.registration?._id && hint?._owner_epoch === this.registration?._owner_epoch) this.dirty = true;
          } catch { /* malformed/unrelated hints never dispatch commands */ }
        });
        await this.wait(wh.connect());
        const token = await this.wait(Promise.resolve(this.options._credential()));
        const auth = await this.wait(wh.auth({ _token: token }));
        if (!auth._ok || !auth._result?._sid) throw new XNetError("E_XNET_FORBIDDEN");
        assertXNetContract("guid", auth._result._user_id); assertXNetContract("guid", auth._result._sid);
        this.authenticatedPrincipal = auth._result._user_id; this.authenticatedSession = auth._result._sid;
        this.setRegistration(await this.call("register-endpoint", { address: this.options._address }));
        if (this.registration!._principal_id !== auth._result._user_id) throw new XNetError("E_XNET_FORBIDDEN");
        this.reconnects = 0; this.rpcFailures = 0; this.lastError = undefined; this.state = "registered"; this.dirty = true;
      }
      if (!this.running || generation !== this.generation) return;
      if (!this.registration || this.registration._lease_expires_at <= this.now()) {
        this.receiveKey = undefined; this.pendingAck = undefined;
        this.setRegistration(await this.call("register-endpoint", { address: this.options._address })); this.dirty = true;
      } else if (this.now() >= this.renewAt) {
        this.setRegistration(await this.call("renew-registration", {
          registration_id: this.registration._id, owner_epoch: this.registration._owner_epoch }));
      }
      for (let count = 0; count < this.options._max_drain! && this.running && generation === this.generation; count++) {
        if (this.pendingAck) {
          const recorded = await this.call("ack-processed", this.pendingAck as unknown as Record<string, unknown>);
          assertXNetContract("status", recorded);
          if (recorded._message_id !== this.pendingAck.message_id || recorded._destination._tenant_id !== this.options._address._tenant_id
            || recorded._destination._endpoint_id !== this.options._address._endpoint_id
            || !recorded._attempts.some((a: { _id: string; _outcome: string }) => a._id === this.pendingAck!.attempt_id && a._outcome === this.pendingAck!.outcome)
            || (this.pendingAck.outcome === "processed" && (recorded._state !== "processed" || recorded._receipt_id !== this.pendingAck.receipt_id))) xnetReject();
          this.pendingAck = undefined; this.receiveKey = undefined; this.dirty = true; this.rpcFailures = 0;
        }
        if (!this.dirty && !this.receiveKey) { this.state = "idle"; break; }
        this.receiveKey ??= _xu.guid();
        const value = await this.call("receive-next", { registration_id: this.registration!._id, request_key: this.receiveKey });
        this.rpcFailures = 0; this.lastError = undefined;
        if (value?._empty === true) { this.receiveKey = undefined; this.dirty = false; this.state = "idle"; break; }
        assertXNetDispatch(value); const dispatch: XNetDispatch = value;
        if (dispatch._registration_id !== this.registration!._id || dispatch._owner_epoch !== this.registration!._owner_epoch
          || dispatch._envelope._tenant_id !== this.options._address._tenant_id
          || dispatch._envelope._destination._endpoint_id !== this.options._address._endpoint_id
          || dispatch._claim_expires_at <= this.now()) throw new XNetError("E_XNET_STALE_CLAIM");
        this.state = "draining";
        const outcome = await this.process(dispatch);
        if (!this.running || generation !== this.generation) return;
        this.pendingAck = { delivery_id: dispatch._delivery_id, message_id: dispatch._envelope._message_id,
          attempt_id: dispatch._attempt_id, registration_id: dispatch._registration_id, owner_epoch: dispatch._owner_epoch,
          claim_token: dispatch._claim_token, ...outcome };
      }
    } catch (e) {
      if (!this.running || generation !== this.generation) return;
      const code = (e as { _code?: string })?._code;
      this.lastError = ["E_XNET_FORBIDDEN", "E_XNET_STALE_CLAIM", "E_XNET_OWNER_CONFLICT", "E_XNET_IDEMPOTENCY_CONFLICT"].includes(code ?? "") ? code : "E_XNET_TRANSPORT";
      if (code === "E_XNET_STALE_CLAIM") { this.receiveKey = undefined; this.pendingAck = undefined; this.dirty = true; }
      else if (code === "E_XNET_IDEMPOTENCY_CONFLICT" || code === "E_XNET_INVALID_MESSAGE") {
        this.wh?.close(); this.running = false; this.state = "failed";
      } else if (!this.wh?.isOpen() || code === "E_XNET_FORBIDDEN" || code === "E_XNET_OWNER_CONFLICT" || ++this.rpcFailures >= 3) this.reconnect();
      // A live connection timeout retains the same receive key or ACK for replay.
    } finally { this.busy = false; }
  }
  private async process(dispatch: XNetDispatch): Promise<Pick<XNetProcessingAck, "outcome" | "receipt_id">> {
    const type = dispatch._envelope._message_type;
    const handler = Object.hasOwn(this.options._handlers, type) ? this.options._handlers[type] : undefined;
    if (!handler) return { outcome: "permanent_failure" };
    try {
      const execute = this.options._execute ?? (command => _x.execute(command));
      const response = await this.wait(Promise.resolve(execute({ _module: handler._module, _op: handler._op,
        _params: { envelope: dispatch._envelope } }))) as XResponseData;
      if (!response?._ok) return { outcome: "uncertain" };
      const result = response._result;
      if (result && ["retryable_failure", "permanent_failure", "uncertain"].includes(result._outcome)
        && Object.keys(result).length === 1) return { outcome: result._outcome };
      assertXNetTargetReceipt(result);
      if (result._tenant_id !== this.options._address._tenant_id || result._endpoint_id !== this.options._address._endpoint_id
        || result._message_id !== dispatch._envelope._message_id || result._idempotency_key !== dispatch._envelope._idempotency_key) return { outcome: "uncertain" };
      return { outcome: "processed", receipt_id: result._id };
    } catch { return { outcome: "uncertain" }; }
  }
}
export function createXNetClient(options: XNetClientOptions): XNetClient { return new XNetClient(options); }
