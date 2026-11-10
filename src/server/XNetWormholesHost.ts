import type { Server } from "node:http";
import { _x, _xu, XError } from "@xpell/core";
import type { XCommandData } from "@xpell/core";
import { createWormholesRestRouter, createWormholesWSServer, wsGetConn, wsSendToWid } from "@xpell/node";
import type { WHContext, WHGatewayOptions, XWebRouteContributor } from "@xpell/node";
import { assertXNetContract, assertXNetJson, XNET_AVAILABLE_EVENT, XNetError, xnetReject } from "../shared/index.js";
import type { XNetEndpointRegistration } from "../shared/index.js";
import { createXNetModule, XNET_OPS } from "./XNetModule.js";
import type { XNetPrincipal, XNetPrincipalGrant, XNetStore } from "./contracts.js";

export interface XNetCredentialIdentity {
  readonly _principal: Pick<XNetPrincipal, "_principal_id" | "_tenant_id">;
  readonly _expires_at: number;
}
export interface XNetWormholesHostOptions {
  readonly _store: XNetStore;
  /** Host-owned, bounded verifier. Called on AUTH and again for every request/frame use. */
  readonly _verify_credential: (token: string) => XNetCredentialIdentity | undefined | Promise<XNetCredentialIdentity | undefined>;
  readonly _clock?: () => number;
  readonly _frame_limit?: number;
  readonly _max_connections?: number;
  readonly _hint_interval_ms?: number;
}
type Session = {
  ctx: WHContext; token: string; identity: XNetCredentialIdentity; principal: XNetPrincipal;
  registration?: XNetEndpointRegistration; nextHint: number;
};
const permission: Record<string, XNetPrincipalGrant['_permissions'][number]> = {
  "get-readiness": "status", submit: "submit", "get-message-status": "status", "ack-processed": "ack",
  "register-endpoint": "receive", "renew-registration": "receive", "unregister-endpoint": "receive", "receive-next": "receive",
};
const denied = () => new XError("E_XNET_FORBIDDEN", "XNet request rejected");

/** Composition adapter, not another XModule or HTTP server. No import/start timers. */
export class XNetWormholesHost {
  readonly _module;
  private readonly options: XNetWormholesHostOptions;
  private readonly sessions = new Map<string, Session>();
  private readonly attestations = new WeakMap<object, Session>();
  private wss?: ReturnType<typeof createWormholesWSServer>;
  private running = false;
  private cleanupCursor = 0;
  private hintCursor = 0;
  private readonly frameLimit: number;
  private readonly maxConnections: number;
  private readonly hintInterval: number;

  constructor(options: XNetWormholesHostOptions) {
    this.options = { ...options };
    this.frameLimit = options._frame_limit ?? 100; this.maxConnections = options._max_connections ?? 1000;
    this.hintInterval = options._hint_interval_ms ?? 1000;
    if (!options._store || typeof options._verify_credential !== "function"
      || ![this.frameLimit, this.maxConnections, this.hintInterval].every(n => Number.isSafeInteger(n) && n > 0)
      || this.frameLimit > 1000 || this.maxConnections > 10_000) xnetReject("E_XNET_INVALID_CONFIG");
    this._module = createXNetModule({ _store: options._store, _advance_on_frame: true,
      _authorize: command => this.consumeAttestation(command),
      _on_registration: (p, r) => {
        const s = p._connection_id && this.sessions.get(p._connection_id);
        if (s && s.principal._session_id === p._session_id) { s.registration = r; s.nextHint = 0; }
      },
      _frame_work: () => this.maintain(),
    });
  }
  private now(): number {
    const now = (this.options._clock ?? Date.now)();
    if (!Number.isSafeInteger(now) || now < 0) xnetReject("E_XNET_INVALID_CLOCK");
    return now;
  }
  /** Pass these to XNode.start; attach/start this adapter on its actual server afterward. */
  nodeOptions() {
    const route: XWebRouteContributor = app => { app.use(createWormholesRestRouter({ ...this.gatewayOptions(), _log: false })); };
    return { _modules: [this._module], _routes: [route], _web_settings: { "enable-wormhole": false as const } };
  }
  async start(server: Server): Promise<void> {
    if (this.running) xnetReject("E_XNET_ALREADY_OPEN");
    if (server.listenerCount("upgrade") !== 0) xnetReject("E_XNET_INVALID_CONFIG");
    const loaded = _x.getModule("xnet");
    if (loaded && loaded !== this._module) xnetReject("E_XNET_OWNER_CONFLICT");
    if (!loaded) await _x.loadModuleAsync(this._module);
    this._module.start(); this.running = true;
    try {
      this.wss = createWormholesWSServer(server, { ...this.gatewayOptions(), _path: "/wh/v2",
        _max_payload_bytes: 128 * 1024, _log_connect: false, _log_messages: false });
      this.wss.on("connection", ws => { if (this.wss!.clients.size > this.maxConnections) ws.terminate(); });
    } catch { this.running = false; this._module.stop(); throw new XNetError("E_XNET_NOT_READY"); }
  }
  async stop(): Promise<void> {
    this.running = false;
    for (const s of this.sessions.values()) this.release(s);
    this.sessions.clear();
    const wss = this.wss; this.wss = undefined;
    if (wss) {
      for (const ws of wss.clients) ws.terminate();
      await new Promise<void>(resolve => wss.close(() => resolve()));
    }
    this._module.stop();
  }
  /** Deterministic alternative to engine frames. The module owns the overlap guard. */
  async advance(): Promise<void> { if (this.running) await this._module.onFrame(0); }

  private async verify(token: string): Promise<XNetCredentialIdentity | undefined> {
    try {
      const identity = await this.options._verify_credential(token);
      if (!identity || !Number.isSafeInteger(identity._expires_at) || identity._expires_at <= this.now()) return;
      assertXNetContract("guid", identity._principal._principal_id); assertXNetContract("guid", identity._principal._tenant_id);
      return { _principal: { _principal_id: identity._principal._principal_id, _tenant_id: identity._principal._tenant_id }, _expires_at: identity._expires_at };
    } catch { return; }
  }
  private current(s: Session): boolean {
    const wid = s.principal._connection_id!;
    return this.running && this.sessions.get(wid) === s && wsGetConn(wid)?._ctx === s.ctx
      && wsGetConn(wid)?._ws.readyState === 1 && s.ctx._sid === s.principal._session_id
      && s.ctx._auth._authenticated === true && s.principal._auth_expires_at! > this.now();
  }
  private async revalidate(s: Session): Promise<boolean> {
    const identity = await this.verify(s.token);
    if (!this.current(s) || !identity || identity._principal._principal_id !== s.principal._principal_id
      || identity._principal._tenant_id !== s.principal._tenant_id) return false;
    // A credential can shorten validity, never silently extend an existing AUTH session.
    s.principal = { ...s.principal, _auth_expires_at: Math.min(s.principal._auth_expires_at!, identity._expires_at) };
    return s.principal._auth_expires_at! > this.now();
  }
  private consumeAttestation(command: Readonly<XCommandData>): XNetPrincipal | undefined {
    const s = this.attestations.get(command); this.attestations.delete(command);
    return s && this.current(s) ? { ...s.principal } : undefined;
  }
  private gatewayOptions(): WHGatewayOptions {
    return { _require_auth: true, _allow_inbound_evt: false,
      _auth: async (payload, ctx) => {
        const wid = ctx._meta?._wid;
        // REST request IDs and client-supplied SID strings cannot manufacture this reference.
        if (!this.running || !wid || wsGetConn(wid)?._ctx !== ctx || typeof payload._token !== "string"
          || !payload._token || payload._token.length > 4096) return { _authenticated: false };
        const identity = await this.verify(payload._token);
        if (!identity || !this.running || wsGetConn(wid)?._ctx !== ctx) return { _authenticated: false };
        const old = this.sessions.get(wid);
        if (old && (old.identity._principal._principal_id !== identity._principal._principal_id
          || old.identity._principal._tenant_id !== identity._principal._tenant_id)) return { _authenticated: false };
        if (!old && this.sessions.size >= this.maxConnections) return { _authenticated: false };
        const sid = old?.principal._session_id ?? _xu.guid();
        const s: Session = { ctx, token: payload._token, identity,
          principal: { ...identity._principal, _connection_id: wid, _session_id: sid, _auth_expires_at: identity._expires_at },
          registration: old?.registration, nextHint: old?.nextHint ?? 0 };
        this.sessions.set(wid, s);
        return { _authenticated: true, _sid: sid, _user_id: identity._principal._principal_id, _auth_type: "xnet-host" };
      },
      _authorize_req: async (command, ctx) => {
        const wid = ctx._meta?._wid; const s = wid && this.sessions.get(wid);
        if (!s || s.ctx !== ctx || !await this.revalidate(s)) throw denied();
        if (!this._module.get_readiness()._ready) throw new XError("E_XNET_NOT_READY", "XNet is not ready");
        if (command._module !== "xnet" || !Object.hasOwn(XNET_OPS, command._op)
          || Object.keys(command).some(k => !["_module", "_op", "_params", "_ctx"].includes(k))) throw denied();
        try {
          this.options._store.authorizePrincipal(s.principal, permission[command._op]);
          if (["register-endpoint", "renew-registration", "receive-next"].includes(command._op)) this.options._store.authorizePrincipal(s.principal, "ack");
        }
        catch { throw denied(); }
        // Remove only canonical gateway-injected metadata. Identity is carried by
        // the private, one-use command reference attestation, never serialized fields.
        if (!command._params || typeof command._params !== "object" || Array.isArray(command._params)) throw denied();
        const params = { ...command._params };
        for (const key of ["_wid", "_sid", "_auth", "_from", "_to"]) delete params[key];
        command._params = params; delete (command as any)._ctx;
        try { assertXNetJson(command, 0, new Set(), 17); } catch { throw denied(); }
        if (command._op === "register-endpoint") await this.releaseDisconnected();
        if (!this.current(s)) throw denied();
        this.attestations.set(command, s);
      },
    };
  }
  private release(s: Session): void {
    if (!s.registration || !this.options._store.isOpen()) return;
    try { this.options._store.unregisterEndpoint(s.principal, s.registration._id, s.registration._owner_epoch); } catch { /* expired/revoked ownership is already fenced */ }
    s.registration = undefined;
  }
  private async releaseDisconnected(): Promise<void> {
    // Fixed-capacity map; each pass visits at most the declared frame budget.
    const entries = [...this.sessions.values()];
    for (let n = 0; n < Math.min(this.frameLimit, entries.length); n++) {
      const s = entries[this.cleanupCursor % entries.length];
      this.cleanupCursor = (this.cleanupCursor + 1) % entries.length;
      if (wsGetConn(s.principal._connection_id!)?._ctx !== s.ctx || wsGetConn(s.principal._connection_id!)?._ws.readyState !== 1) {
        this.release(s); this.sessions.delete(s.principal._connection_id!);
      }
    }
  }
  private async maintain(): Promise<void> {
    if (!this.running) return;
    await this.releaseDisconnected();
    const entries = [...this.sessions.values()];
    for (let n = 0; n < Math.min(this.frameLimit, entries.length); n++) {
      const s = entries[this.hintCursor % entries.length];
      this.hintCursor = (this.hintCursor + 1) % entries.length;
      if (!await this.revalidate(s)) {
        if (!this.running) return;
        this.release(s); s.ctx._auth = { _authenticated: false };
        this.sessions.delete(s.principal._connection_id!); wsGetConn(s.principal._connection_id!)?._ws.terminate(); continue;
      }
      if (!this.running || !s.registration || s.nextHint > this.now()) continue;
      try {
        if (this.options._store.hasPending(s.principal, s.registration._id)) {
          wsSendToWid(s.principal._connection_id!, { _name: XNET_AVAILABLE_EVENT,
            _args: [{ _registration_id: s.registration._id, _owner_epoch: s.registration._owner_epoch }] });
        }
      } catch { /* stale/revoked endpoints receive no payload or hint */ }
      s.nextHint = this.now() + this.hintInterval;
    }
  }
}
export function createXNetWormholesHost(options: XNetWormholesHostOptions): XNetWormholesHost { return new XNetWormholesHost(options); }
