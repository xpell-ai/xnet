import Database from "better-sqlite3";
import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { existsSync } from "node:fs";
import { link, mkdtemp, open, rm } from "node:fs/promises";
import path from "node:path";
import { _xu } from "@xpell/core";
import {
  XNET_LIMITS, XNetError, xnetReject, assertXNetAddress, assertXNetBindingSource,
  assertXNetContract, assertXNetDispatch, assertXNetEndpointIdentity, assertXNetEnvelope,
  assertXNetJson, assertXNetProcessingAck, assertXNetRegistration, assertXNetRouteBinding, assertXNetSubmitIntent,
} from "../shared/index.js";
import type {
  XNetAcceptance, XNetAddress, XNetBindingSource, XNetDispatch, XNetEndpointIdentity,
  XNetEndpointRegistration, XNetMessageEnvelope, XNetMessageStatus, XNetProcessingAck, XNetRouteBinding, XNetSubmitIntent,
} from "../shared/index.js";
import type { XNetPrincipal, XNetPrincipalGrant, XNetProvisioning, XNetStore, XNetStoreOptions } from "./contracts.js";
import { XNET_APPLICATION_ID, XNET_SCHEMA_V1, XNET_SCHEMA_V2, XNET_SCHEMA_VERSION } from "./schema.js";

type Row = Record<string, any>;
const hash = (s: string) => createHash("sha256").update(s).digest("hex");
function canonical(v: any): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
  return `{${Object.keys(v).sort().map(k => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`;
}
function sourceKey(s: XNetBindingSource): string {
  return s._kind === "endpoint" ? JSON.stringify([s._kind, s._address._endpoint_id])
    : JSON.stringify([s._kind, s._adapter_id, s._account_binding_id]);
}

/** Single-server local mailbox. All methods except backup are synchronous bounded units. */
export class SQLiteXNetStore implements XNetStore {
  private db?: Database.Database;
  private readonly options: XNetStoreOptions;
  private readonly claimKey: Buffer;
  private readonly limits: typeof XNET_LIMITS;
  private readonly instanceId: string;

  constructor(options: XNetStoreOptions) {
    if (!options?._db_path || options._db_path === ":memory:" || !path.isAbsolute(options._db_path)
      || !(options._claim_key instanceof Uint8Array) || options._claim_key.length < 32) xnetReject("E_XNET_INVALID_CONFIG");
    this.options = { ...options };
    this.claimKey = Buffer.from(options._claim_key);
    this.instanceId = options._instance_id ?? _xu.guid();
    assertXNetContract("guid", this.instanceId);
    this.limits = Object.freeze({ ...XNET_LIMITS, ...options._limits });
    if (Object.keys(options._limits ?? {}).some(k => !Object.hasOwn(XNET_LIMITS, k))
      || Object.values(this.limits).some(n => !Number.isSafeInteger(n) || n <= 0)
      || this.limits._max_batch > 100 || this.limits._max_envelope_bytes > 64 * 1024
      || this.limits._advance_limit > 1000
      || this.limits._terminal_payload_retention_ms > XNET_LIMITS._terminal_payload_retention_ms) xnetReject("E_XNET_INVALID_CONFIG");
  }

  private now(): number {
    const n = (this.options._clock ?? Date.now)();
    if (!Number.isSafeInteger(n) || n < 0 || n > Number.MAX_SAFE_INTEGER - this.limits._ttl_ms) xnetReject("E_XNET_INVALID_CLOCK");
    return n;
  }
  private connection(): Database.Database {
    if (!this.db?.open) xnetReject("E_XNET_NOT_READY");
    return this.db;
  }
  private get(sql: string, ...values: any[]): Row | undefined {
    return this.connection().prepare(sql).get(...values) as Row | undefined;
  }
  private run(sql: string, ...values: any[]): Database.RunResult { return this.connection().prepare(sql).run(...values); }
  private tx<T>(fn: () => T): T {
    try { return this.connection().transaction(fn).immediate(); }
    catch (e) { if (e instanceof XNetError) throw e; throw new XNetError("E_XNET_STORE_UNAVAILABLE", "XNet transaction failed"); }
  }
  private configure(db: Database.Database): void {
    db.pragma("busy_timeout = 1000");
    db.pragma("foreign_keys = ON");
    db.pragma("journal_mode = WAL");
    db.pragma("synchronous = FULL");
  }
  private checkOwnership(db: Database.Database, allow_empty: boolean): number {
    const version = db.pragma("user_version", { simple: true }) as number;
    const owner = db.pragma("application_id", { simple: true });
    const tables = db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").get() as Row;
    if (allow_empty && version === 0 && owner === 0 && tables.n === 0) return 0;
    if (owner !== XNET_APPLICATION_ID || (version !== XNET_SCHEMA_VERSION && !(allow_empty && version === 1))) xnetReject("E_XNET_SCHEMA_VERSION");
    return version;
  }
  /** Explicit initialization/migration only. V1 -> V2 adds metadata without rewriting historical rows. */
  initialize(): void {
    if (this.isOpen()) xnetReject("E_XNET_ALREADY_OPEN");
    const db = new Database(this.options._db_path);
    try {
      const version = this.checkOwnership(db, true);
      this.configure(db);
      if (version < XNET_SCHEMA_VERSION) db.transaction(() => {
        if (version === 0) db.exec(XNET_SCHEMA_V1);
        db.exec(XNET_SCHEMA_V2);
        db.pragma(`application_id = ${XNET_APPLICATION_ID}`);
        db.pragma(`user_version = ${XNET_SCHEMA_VERSION}`);
      }).immediate();
      this.db = db;
    } catch (e) { db.close(); if (e instanceof XNetError) throw e; throw new XNetError("E_XNET_STORE_UNAVAILABLE"); }
  }
  /** Opens only an already initialized supported schema; never migrates. */
  open(): void {
    if (this.isOpen()) return;
    let db: Database.Database | undefined;
    try {
      db = new Database(this.options._db_path, { fileMustExist: true });
      this.checkOwnership(db, false); this.configure(db); this.db = db;
    } catch (e) { db?.close(); if (e instanceof XNetError) throw e; throw new XNetError("E_XNET_NOT_READY"); }
  }
  close(): void { this.db?.close(); this.db = undefined; }
  isOpen(): boolean { return this.db?.open === true; }
  getDiagnostics() {
    const d = this.connection();
    return { _schema_version: d.pragma("user_version", { simple: true }),
      _journal_mode: d.pragma("journal_mode", { simple: true }),
      _synchronous: d.pragma("synchronous", { simple: true }), _foreign_keys: d.pragma("foreign_keys", { simple: true }) };
  }
  async backup(destination: string): Promise<void> {
    if (!path.isAbsolute(destination) || existsSync(destination) || path.resolve(destination) === path.resolve(this.options._db_path)) xnetReject("E_XNET_INVALID_CONFIG");
    let directory: string | undefined;
    try {
      // SQLite writes only inside a newly created private directory on the target
      // filesystem. link() publishes a complete snapshot atomically, without replace.
      const db = this.connection();
      directory = await mkdtemp(path.join(path.dirname(destination), ".xnet-backup-"));
      const snapshot = path.join(directory, "snapshot.sqlite");
      await db.backup(snapshot);
      const file = await open(snapshot, "r");
      try { await file.sync(); } finally { await file.close(); }
      await link(snapshot, destination);
      const parent = await open(path.dirname(destination), "r");
      try { await parent.sync(); } finally { await parent.close(); }
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === "EEXIST") throw new XNetError("E_XNET_INVALID_CONFIG");
      throw new XNetError("E_XNET_STORE_UNAVAILABLE", "XNet backup failed");
    } finally {
      if (directory) {
        try { await rm(directory, { recursive: true, force: true }); }
        catch { throw new XNetError("E_XNET_STORE_UNAVAILABLE", "XNet backup cleanup failed"); }
      }
    }
  }

  private endpoint(tenant: string, id: string): XNetEndpointIdentity {
    const r = this.get("SELECT definition FROM xnet_endpoint WHERE tenant_id=? AND id=?", tenant, id);
    if (!r) xnetReject("E_XNET_FORBIDDEN");
    const e: XNetEndpointIdentity = JSON.parse(r.definition);
    if (!e._enabled) xnetReject("E_XNET_FORBIDDEN");
    return e;
  }
  private checkSource(tenant: string, s: XNetBindingSource): void {
    assertXNetBindingSource(s);
    if (s._kind === "endpoint") {
      if (s._address._tenant_id !== tenant) xnetReject("E_XNET_FORBIDDEN");
      this.endpoint(tenant, s._address._endpoint_id);
    } else {
      const a = this.get("SELECT definition FROM xnet_account WHERE tenant_id=? AND id=? AND adapter_id=?", tenant, s._account_binding_id, s._adapter_id);
      if (!a || !JSON.parse(a.definition)._enabled) xnetReject("E_XNET_FORBIDDEN");
    }
  }
  /** Host composition only, never reachable through XNET_OPS. Entire fixture/config is atomic. */
  provision(input: XNetProvisioning): void {
    assertXNetJson(input);
    this.tx(() => {
      for (const tenant of input.tenants) { assertXNetContract("guid", tenant); this.run("INSERT INTO xnet_tenant VALUES (?)", tenant); }
      for (const e of input.endpoints) {
        assertXNetEndpointIdentity(e);
        this.run("INSERT INTO xnet_endpoint(tenant_id,id,kind,name,definition) VALUES (?,?,?,?,?)", e._address._tenant_id, e._id, e._kind, e._name, JSON.stringify(e));
      }
      for (const a of input.accounts) {
        assertXNetContract("guid", a._id); assertXNetContract("guid", a._tenant_id);
        if (!/^[a-z0-9-]+$/u.test(a._adapter_id) || !/^[a-z0-9-]+$/u.test(a._provider)
          || typeof a._account_locator !== "string" || !a._account_locator || a._account_locator.length > 256 || typeof a._enabled !== "boolean") xnetReject();
        this.run("INSERT INTO xnet_account VALUES (?,?,?,?,?,?)", a._tenant_id, a._id, a._adapter_id, a._provider, a._account_locator, JSON.stringify(a));
      }
      for (const g of input.grants) {
        assertXNetContract("guid", g._principal_id); assertXNetContract("guid", g._tenant_id);
        if (typeof g._enabled !== "boolean" || !Array.isArray(g._permissions) || g._permissions.some(p => !["submit", "receive", "ack", "status"].includes(p))
          || !Array.isArray(g._receive_endpoint_ids)) xnetReject();
        if (g._source) this.checkSource(g._tenant_id, g._source);
        for (const id of g._receive_endpoint_ids) { assertXNetContract("guid", id); this.endpoint(g._tenant_id, id); }
        this.run("INSERT INTO xnet_grant VALUES (?,?,?)", g._tenant_id, g._principal_id, JSON.stringify(g));
      }
      for (const r of input.routes) this.writeRoute(r, 0);
    });
  }
  private writeRoute(r: XNetRouteBinding, expected: number): void {
    assertXNetRouteBinding(r); this.checkSource(r._tenant_id, r._source);
    const endpoint = this.endpoint(r._tenant_id, r._destination._endpoint_id);
    if (!endpoint._accepted_message_types.includes(r._message_type)) xnetReject("E_XNET_FORBIDDEN");
    const old = this.get("SELECT version FROM xnet_route WHERE tenant_id=? AND id=?", r._tenant_id, r._id);
    if ((old?.version ?? 0) !== expected || r._version !== expected + 1) xnetReject("E_XNET_VERSION_CONFLICT");
    this.run(`INSERT INTO xnet_route VALUES (?,?,?,?,?,?,?) ON CONFLICT(tenant_id,id) DO UPDATE SET
      source_key=excluded.source_key,message_type=excluded.message_type,endpoint_id=excluded.endpoint_id,
      version=excluded.version,definition=excluded.definition`, r._tenant_id, r._id, sourceKey(r._source), r._message_type,
    r._destination._endpoint_id, r._version, JSON.stringify(r));
  }
  setRouteBinding(binding: XNetRouteBinding, expected_version: number): void {
    if (!Number.isSafeInteger(expected_version) || expected_version < 0) xnetReject();
    this.tx(() => this.writeRoute(binding, expected_version));
  }
  private grant(p: XNetPrincipal, op: XNetPrincipalGrant['_permissions'][number]): XNetPrincipalGrant {
    if (!p || typeof p !== "object") xnetReject("E_XNET_FORBIDDEN");
    assertXNetContract("guid", p._principal_id); assertXNetContract("guid", p._tenant_id);
    const r = this.get("SELECT definition FROM xnet_grant WHERE tenant_id=? AND principal_id=?", p._tenant_id, p._principal_id);
    const g: XNetPrincipalGrant | undefined = r && JSON.parse(r.definition);
    if (!g?._enabled || !g._permissions.includes(op)) xnetReject("E_XNET_FORBIDDEN");
    this.connectionIdentity(p);
    return g;
  }
  authorizePrincipal(p: XNetPrincipal, permission: XNetPrincipalGrant['_permissions'][number]): void {
    this.tx(() => { this.grant(p, permission); });
  }
  private connectionIdentity(p: XNetPrincipal): { _connection_id: string; _session_id: string } | undefined {
    if (p._connection_id === undefined && p._session_id === undefined && p._auth_expires_at === undefined) return;
    assertXNetContract("guid", p._connection_id); assertXNetContract("guid", p._session_id);
    if (!Number.isSafeInteger(p._auth_expires_at) || p._auth_expires_at! <= this.now()) xnetReject("E_XNET_FORBIDDEN");
    return { _connection_id: p._connection_id!, _session_id: p._session_id! };
  }

  accept(principal: XNetPrincipal, intent: XNetSubmitIntent): XNetAcceptance {
    return this.acceptBatch(principal, [intent])[0];
  }
  acceptBatch(principal: XNetPrincipal, intents: readonly XNetSubmitIntent[]): XNetAcceptance[] {
    if (!Array.isArray(intents) || !intents.length || intents.length > this.limits._max_batch) xnetReject();
    intents.forEach(assertXNetSubmitIntent);
    // Snapshot data before a host failure hook can mutate caller-owned objects.
    const batch: XNetSubmitIntent[] = JSON.parse(JSON.stringify(intents));
    return this.tx(() => {
      const g = this.grant(principal, "submit");
      if (!g._source) xnetReject("E_XNET_FORBIDDEN");
      this.checkSource(g._tenant_id, g._source);
      const now = this.now();
      return batch.map(intent => this.acceptOne(g, intent, now));
    });
  }
  private acceptOne(g: XNetPrincipalGrant, intent: XNetSubmitIntent, now: number): XNetAcceptance {
    const source = g._source!;
    if (intent.destination && (source._kind !== "endpoint" || intent.destination._tenant_id !== g._tenant_id)) xnetReject("E_XNET_FORBIDDEN");
    if (source._kind === "provider_account" && !intent.event_id) xnetReject();
    const key = sourceKey(source);
    const dedup = source._kind === "provider_account" ? `${intent.message_type}:${intent.event_id}` : intent.idempotency_key;
    const semantic = { message_type: intent.message_type, payload: intent.payload,
      destination: intent.destination ?? null, causation_id: intent.causation_id ?? null, ttl_ms: intent.ttl_ms ?? null };
    const digest = hash(canonical(semantic));
    const old = this.connection().prepare(`SELECT m.*,coalesce(l.hash_version,1) AS hash_version FROM xnet_message m
      LEFT JOIN xnet_message_lifecycle l ON l.tenant_id=m.tenant_id AND l.message_id=m.id
      WHERE m.tenant_id=? AND m.source_key=? AND (m.dedup_key=? OR m.idempotency_key=?)`).all(g._tenant_id, key, dedup, intent.idempotency_key) as Row[];
    if (old.length) {
      // V1 did not distinguish an omitted TTL from its resolved default. Compare
      // using that row's original duration, never the current host configuration.
      const previous = old[0];
      const expected = previous.hash_version === 1
        ? hash(canonical({ ...semantic, ttl_ms: intent.ttl_ms ?? previous.expires_at - previous.accepted_at })) : digest;
      if (old.length !== 1 || previous.content_hash !== expected || previous.dedup_key !== dedup || previous.idempotency_key !== intent.idempotency_key) xnetReject("E_XNET_IDEMPOTENCY_CONFLICT");
      return this.acceptance(old[0], true);
    }
    const r = this.get("SELECT * FROM xnet_route WHERE tenant_id=? AND source_key=? AND message_type=?", g._tenant_id, key, intent.message_type);
    if (!r) xnetReject("E_XNET_FORBIDDEN");
    const binding: XNetRouteBinding = JSON.parse(r.definition);
    if (!binding._enabled || (intent.destination && intent.destination._endpoint_id !== r.endpoint_id)) xnetReject("E_XNET_FORBIDDEN");
    const e = this.endpoint(g._tenant_id, r.endpoint_id);
    if (!e._accepted_message_types.includes(intent.message_type)) xnetReject("E_XNET_FORBIDDEN");
    if (intent.causation_id) {
      // Applies the same source/destination access policy as a status read, inside this transaction.
      this.authorizedMessage(g, intent.causation_id);
    }
    const counts = this.get(`SELECT count(*) AS total, coalesce(sum(tenant_id=?),0) AS tenant FROM xnet_delivery
      WHERE state IN ('queued','leased','dispatched','retry_wait','uncertain')`, g._tenant_id)!;
    if (counts.total >= this.limits._max_queued_server || counts.tenant >= this.limits._max_queued_per_tenant) xnetReject("E_XNET_CAPACITY");
    const ttl = intent.ttl_ms ?? this.limits._ttl_ms;
    if (ttl > this.limits._ttl_ms) xnetReject();
    const envelope: XNetMessageEnvelope = {
      _envelope_version: 1, _message_id: _xu.guid(), _tenant_id: g._tenant_id,
      _source: { ...source, _principal_id: g._principal_id }, _destination: binding._destination,
      _message_type: intent.message_type, _correlation_id: intent.correlation_id ?? _xu.guid(),
      ...(intent.causation_id ? { _causation_id: intent.causation_id } : {}),
      ...(intent.event_id ? { _event_id: intent.event_id } : {}), _dedup_key: dedup,
      _idempotency_key: intent.idempotency_key, _created_at: now, _expires_at: now + ttl,
      _route_binding_id: binding._id, _route_binding_version: binding._version,
      _payload: intent.payload, _metadata: {},
    };
    assertXNetEnvelope(envelope);
    const json = JSON.stringify(envelope); const bytes = Buffer.byteLength(json);
    if (bytes > this.limits._max_envelope_bytes) xnetReject("E_XNET_TOO_LARGE");
    this.run("INSERT INTO xnet_message VALUES (?,?,?,?,?,?,?,?,?,?,?)", g._tenant_id, envelope._message_id,
      key, dedup, intent.idempotency_key, digest, r.endpoint_id, now, envelope._expires_at, bytes, json);
    this.run("INSERT INTO xnet_message_lifecycle(tenant_id,message_id,hash_version) VALUES (?,?,2)", g._tenant_id, envelope._message_id);
    this.options._fault?.("accept.after-message");
    this.run("INSERT INTO xnet_delivery(tenant_id,id,message_id,endpoint_id,state,next_attempt_at) VALUES (?,?,?,?,?,?)",
      g._tenant_id, _xu.guid(), envelope._message_id, r.endpoint_id, "queued", now);
    this.options._fault?.("accept.after-delivery");
    return { _message_id: envelope._message_id, _stage: "durably_accepted", _duplicate: false, _accepted_at: now, _expires_at: envelope._expires_at };
  }
  private acceptance(row: Row, duplicate: boolean): XNetAcceptance {
    return { _message_id: row.id, _stage: "durably_accepted", _duplicate: duplicate, _accepted_at: row.accepted_at, _expires_at: row.expires_at };
  }

  registerEndpoint(p: XNetPrincipal, address: XNetAddress): XNetEndpointRegistration {
    assertXNetAddress(address);
    return this.tx(() => {
      const g = this.grant(p, "receive");
      if (g._tenant_id !== address._tenant_id || !g._receive_endpoint_ids.includes(address._endpoint_id)) xnetReject("E_XNET_FORBIDDEN");
      this.endpoint(g._tenant_id, address._endpoint_id);
      const now = this.now();
      const connection = this.connectionIdentity(p);
      const old = this.get("SELECT * FROM xnet_registration WHERE tenant_id=? AND endpoint_id=? AND active=1", g._tenant_id, address._endpoint_id);
      if (old && old.instance_id === this.instanceId && old.expires_at > now) {
        const definition: XNetEndpointRegistration = JSON.parse(old.definition);
        if (old.principal_id !== p._principal_id || definition._connection_id !== connection?._connection_id
          || definition._session_id !== connection?._session_id) xnetReject("E_XNET_OWNER_CONFLICT");
        return JSON.parse(old.definition);
      }
      if (old) this.run("UPDATE xnet_registration SET active=0 WHERE tenant_id=? AND id=?", g._tenant_id, old.id);
      this.run("UPDATE xnet_endpoint SET epoch=epoch+1 WHERE tenant_id=? AND id=?", g._tenant_id, address._endpoint_id);
      const epoch = this.get("SELECT epoch FROM xnet_endpoint WHERE tenant_id=? AND id=?", g._tenant_id, address._endpoint_id)!.epoch;
      const registration: XNetEndpointRegistration = { _id: _xu.guid(), _endpoint: address, _principal_id: p._principal_id,
        _runtime_instance_id: this.instanceId, _owner_instance_id: this.instanceId, _owner_epoch: epoch,
        ...connection, _registered_at: now,
        _lease_expires_at: Math.min(now + this.limits._registration_lease_ms, p._auth_expires_at ?? Number.MAX_SAFE_INTEGER) };
      assertXNetRegistration(registration);
      this.run("INSERT INTO xnet_registration VALUES (?,?,?,?,?,?,?,?,1)", g._tenant_id, registration._id,
        address._endpoint_id, p._principal_id, this.instanceId, epoch, registration._lease_expires_at, JSON.stringify(registration));
      return registration;
    });
  }
  private registration(p: XNetPrincipal, id: string, op: "receive" | "ack"): Row {
    assertXNetContract("guid", id);
    const g = this.grant(p, op);
    const r = this.get("SELECT * FROM xnet_registration WHERE tenant_id=? AND id=?", g._tenant_id, id);
    if (!r || !r.active || r.principal_id !== p._principal_id || r.instance_id !== this.instanceId
      || r.expires_at <= this.now() || !g._receive_endpoint_ids.includes(r.endpoint_id)) xnetReject("E_XNET_STALE_CLAIM");
    const connection = this.connectionIdentity(p);
    const definition: XNetEndpointRegistration = JSON.parse(r.definition);
    if (definition._connection_id !== connection?._connection_id || definition._session_id !== connection?._session_id) xnetReject("E_XNET_STALE_CLAIM");
    this.endpoint(g._tenant_id, r.endpoint_id);
    return r;
  }
  renewRegistration(p: XNetPrincipal, id: string, owner_epoch: number): XNetEndpointRegistration {
    assertXNetContract("guid", id);
    return this.tx(() => {
      const r = this.registration(p, id, "receive");
      if (r.epoch !== owner_epoch) xnetReject("E_XNET_STALE_CLAIM");
      const renewed: XNetEndpointRegistration = { ...JSON.parse(r.definition),
        _lease_expires_at: Math.min(this.now() + this.limits._registration_lease_ms, p._auth_expires_at ?? Number.MAX_SAFE_INTEGER) };
      assertXNetRegistration(renewed);
      this.run("UPDATE xnet_registration SET expires_at=?,definition=? WHERE tenant_id=? AND id=? AND active=1",
        renewed._lease_expires_at, JSON.stringify(renewed), p._tenant_id, id);
      return renewed;
    });
  }
  unregisterEndpoint(p: XNetPrincipal, id: string, owner_epoch: number): void {
    this.tx(() => {
      const r = this.registration(p, id, "receive");
      if (r.epoch !== owner_epoch) xnetReject("E_XNET_STALE_CLAIM");
      this.run("UPDATE xnet_registration SET active=0 WHERE tenant_id=? AND id=?", p._tenant_id, id);
    });
  }
  hasPending(p: XNetPrincipal, registration_id: string): boolean {
    return this.tx(() => {
      const r = this.registration(p, registration_id, "receive"); const now = this.now();
      return !!this.get(`SELECT d.id FROM xnet_delivery d JOIN xnet_message m ON m.tenant_id=d.tenant_id AND m.id=d.message_id
        WHERE d.tenant_id=? AND d.endpoint_id=? AND d.state='queued' AND d.next_attempt_at<=? AND m.expires_at>?
        AND d.attempt_count<? LIMIT 1`, p._tenant_id, r.endpoint_id, now, now, this.limits._max_attempts);
    });
  }
  private token(attempt: Row): string {
    return createHmac("sha256", this.claimKey).update(canonical([1, attempt.tenant_id, attempt.delivery_id,
      attempt.id, attempt.registration_id, attempt.epoch, attempt.nonce])).digest("base64url");
  }
  private dispatch(delivery: Row, attempt: Row, registration_expires_at: number): XNetDispatch {
    const m = this.get("SELECT envelope FROM xnet_message WHERE tenant_id=? AND id=?", delivery.tenant_id, delivery.message_id)!;
    return { _envelope: JSON.parse(m.envelope), _delivery_id: delivery.id, _attempt_id: attempt.id,
      _registration_id: attempt.registration_id, _owner_epoch: attempt.epoch,
      _claim_token: this.token(attempt), _claim_expires_at: Math.min(attempt.expires_at, registration_expires_at) };
  }
  claimNext(p: XNetPrincipal, registration_id: string, request_key: string): XNetDispatch | null {
    if (typeof request_key !== "string" || !request_key || request_key.length > 256) xnetReject();
    return this.tx(() => {
      const reg = this.registration(p, registration_id, "receive"); const now = this.now();
      const previous = this.get("SELECT * FROM xnet_attempt WHERE tenant_id=? AND registration_id=? AND request_key=?", p._tenant_id, reg.id, request_key);
      if (previous) {
        const d = this.get("SELECT * FROM xnet_delivery WHERE tenant_id=? AND id=?", p._tenant_id, previous.delivery_id)!;
        if (d.attempt_id !== previous.id || previous.expires_at <= now || !["leased", "dispatched"].includes(d.state)) xnetReject("E_XNET_STALE_CLAIM");
        if (hash(this.token(previous)) !== previous.token_hash) xnetReject("E_XNET_STALE_CLAIM");
        return this.dispatch(d, previous, Math.min(reg.expires_at, p._auth_expires_at ?? Number.MAX_SAFE_INTEGER));
      }
      if (this.get("SELECT id FROM xnet_delivery WHERE tenant_id=? AND endpoint_id=? AND state IN ('leased','dispatched')", p._tenant_id, reg.endpoint_id)) return null;
      const d = this.get(`SELECT d.*, m.expires_at FROM xnet_delivery d JOIN xnet_message m ON m.tenant_id=d.tenant_id AND m.id=d.message_id
        WHERE d.tenant_id=? AND d.endpoint_id=? AND d.state='queued' AND d.next_attempt_at<=?
        AND m.expires_at>? AND d.attempt_count<? ORDER BY d.next_attempt_at,m.accepted_at,d.id LIMIT 1`, p._tenant_id, reg.endpoint_id, now, now, this.limits._max_attempts);
      if (!d) return null;
      const attempt: Row = { tenant_id: p._tenant_id, id: _xu.guid(), delivery_id: d.id, attempt_no: d.attempt_count + 1,
        registration_id: reg.id, epoch: reg.epoch, request_key, nonce: randomBytes(32).toString("hex"),
        started_at: now, expires_at: Math.min(now + this.limits._claim_lease_ms, d.expires_at, reg.expires_at,
          p._auth_expires_at ?? Number.MAX_SAFE_INTEGER), outcome: "leased" };
      attempt.token_hash = hash(this.token(attempt));
      this.run("INSERT INTO xnet_attempt VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", attempt.tenant_id, attempt.id, attempt.delivery_id,
        attempt.attempt_no, attempt.registration_id, attempt.epoch, request_key, attempt.nonce, attempt.token_hash, now, attempt.expires_at, "leased");
      this.options._fault?.("claim.after-attempt");
      const result = this.run(`UPDATE xnet_delivery SET state='leased',version=version+1,attempt_count=?,claim_expires_at=?,
        attempt_id=?,registration_id=?,epoch=? WHERE tenant_id=? AND id=? AND state='queued' AND version=?`,
      attempt.attempt_no, attempt.expires_at, attempt.id, reg.id, reg.epoch, p._tenant_id, d.id, d.version);
      if (result.changes !== 1) xnetReject("E_XNET_STALE_CLAIM");
      return this.dispatch({ ...d, attempt_id: attempt.id }, attempt, reg.expires_at);
    });
  }

  private validateClaim(p: XNetPrincipal, a: XNetProcessingAck, require_live: boolean): { d: Row; attempt: Row } {
    const reg = this.registration(p, a.registration_id, "ack");
    const d = this.get("SELECT * FROM xnet_delivery WHERE tenant_id=? AND id=? AND message_id=?", p._tenant_id, a.delivery_id, a.message_id);
    const attempt = this.get("SELECT * FROM xnet_attempt WHERE tenant_id=? AND id=?", p._tenant_id, a.attempt_id);
    const actualHash = Buffer.from(hash(a.claim_token), "hex");
    if (!d || !attempt || d.endpoint_id !== reg.endpoint_id || d.attempt_id !== attempt.id || attempt.delivery_id !== d.id
      || attempt.registration_id !== reg.id || reg.epoch !== a.owner_epoch || attempt.epoch !== a.owner_epoch
      || !timingSafeEqual(actualHash, Buffer.from(attempt.token_hash, "hex"))) xnetReject("E_XNET_STALE_CLAIM");
    if (require_live && (attempt.expires_at <= this.now() || !["leased", "dispatched"].includes(d.state))) xnetReject("E_XNET_STALE_CLAIM");
    return { d, attempt };
  }
  markDispatched(p: XNetPrincipal, dispatch: XNetDispatch): void {
    assertXNetDispatch(dispatch);
    this.tx(() => {
      const ack = { delivery_id: dispatch._delivery_id, message_id: dispatch._envelope._message_id,
        attempt_id: dispatch._attempt_id, registration_id: dispatch._registration_id,
        owner_epoch: dispatch._owner_epoch, claim_token: dispatch._claim_token, outcome: "retryable_failure" as const };
      const { d, attempt } = this.validateClaim(p, ack, true);
      this.run("UPDATE xnet_delivery SET state='dispatched',version=version+1 WHERE tenant_id=? AND id=?", p._tenant_id, d.id);
      this.run("UPDATE xnet_attempt SET outcome='dispatched' WHERE tenant_id=? AND id=?", p._tenant_id, attempt.id);
    });
  }
  acknowledge(p: XNetPrincipal, ack: XNetProcessingAck): XNetMessageStatus {
    assertXNetProcessingAck(ack);
    return this.tx(() => {
      const { d, attempt } = this.validateClaim(p, ack, false);
      const semantic = canonical({ outcome: ack.outcome, receipt_id: ack.receipt_id ?? null, error_code: ack.error_code ?? null });
      const previous = this.get("SELECT ack_json FROM xnet_ack WHERE tenant_id=? AND attempt_id=?", p._tenant_id, attempt.id);
      if (previous) {
        if (previous.ack_json !== semantic) xnetReject("E_XNET_IDEMPOTENCY_CONFLICT");
        return this.statusRow(p._tenant_id, d.message_id);
      }
      if (attempt.expires_at <= this.now() || d.state !== "dispatched") xnetReject("E_XNET_STALE_CLAIM");
      if (ack.receipt_id && this.get("SELECT attempt_id FROM xnet_ack WHERE tenant_id=? AND receipt_id=?", p._tenant_id, ack.receipt_id)) xnetReject("E_XNET_IDEMPOTENCY_CONFLICT");
      const now = this.now();
      this.run("INSERT INTO xnet_ack VALUES (?,?,?,?,?,?,?)", p._tenant_id, attempt.id, d.id, ack.receipt_id ?? null, ack.outcome, semantic, now);
      this.options._fault?.("ack.after-receipt");
      const state = ack.outcome === "processed" ? "processed" : ack.outcome === "uncertain" ? "uncertain"
        : ack.outcome === "permanent_failure" || d.attempt_count >= this.limits._max_attempts ? "dead_letter" : "retry_wait";
      this.run("UPDATE xnet_delivery SET state=?,version=version+1,next_attempt_at=?,receipt_id=? WHERE tenant_id=? AND id=?",
        state, now + this.backoff(d.attempt_count), ack.receipt_id ?? null, p._tenant_id, d.id);
      this.run("UPDATE xnet_attempt SET outcome=? WHERE tenant_id=? AND id=?", ack.outcome, p._tenant_id, attempt.id);
      if (["processed", "dead_letter"].includes(state)) this.recordTerminal(p._tenant_id, d.message_id, now);
      return this.statusRow(p._tenant_id, d.message_id);
    });
  }
  private backoff(attempt: number): number { return Math.min(this.limits._retry_max_ms, this.limits._retry_base_ms * 2 ** Math.min(attempt - 1, 30)); }
  advance(limit = this.limits._advance_limit): number {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > this.limits._advance_limit) xnetReject();
    return this.tx(() => {
      const now = this.now();
      const rows = this.connection().prepare(`SELECT d.*,m.expires_at FROM xnet_delivery d JOIN xnet_message m ON m.tenant_id=d.tenant_id AND m.id=d.message_id
        WHERE d.state IN ('queued','leased','dispatched','retry_wait') AND
        (m.expires_at<=? OR (d.state IN ('leased','dispatched') AND d.claim_expires_at<=?) OR (d.state='retry_wait' AND d.next_attempt_at<=?))
        ORDER BY min(m.expires_at,coalesce(d.claim_expires_at,d.next_attempt_at)),d.id LIMIT ?`).all(now, now, now, limit) as Row[];
      for (const d of rows) {
        const expired = d.expires_at <= now;
        const lease = ["leased", "dispatched"].includes(d.state);
        const state = expired ? "expired" : d.attempt_count >= this.limits._max_attempts ? "dead_letter" : lease ? "retry_wait" : "queued";
        const due = lease ? now + this.backoff(d.attempt_count) : d.next_attempt_at;
        this.run("UPDATE xnet_delivery SET state=?,version=version+1,next_attempt_at=? WHERE tenant_id=? AND id=? AND version=?", state, due, d.tenant_id, d.id, d.version);
        if (lease) this.run("UPDATE xnet_attempt SET outcome=? WHERE tenant_id=? AND id=?", expired ? "expired" : "lease_expired", d.tenant_id, d.attempt_id);
        if (["expired", "dead_letter"].includes(state)) this.recordTerminal(d.tenant_id, d.message_id, now);
      }
      return rows.length;
    });
  }

  private recordTerminal(tenant: string, message: string, now: number): void {
    this.run(`INSERT INTO xnet_message_lifecycle(tenant_id,message_id,hash_version,terminal_at) VALUES (?,?,1,?)
      ON CONFLICT(tenant_id,message_id) DO UPDATE SET terminal_at=coalesce(terminal_at,excluded.terminal_at)`, tenant, message, now);
  }
  /** Explicit bounded payload redaction, not evidence deletion or uncertain reconciliation. */
  cleanupTerminalPayloads(limit = this.limits._advance_limit): number {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > this.limits._advance_limit) xnetReject();
    return this.tx(() => {
      const now = this.now();
      const rows = this.connection().prepare(`SELECT m.tenant_id,m.id,m.envelope,l.terminal_at FROM xnet_message m
        JOIN xnet_delivery d ON d.tenant_id=m.tenant_id AND d.message_id=m.id
        LEFT JOIN xnet_message_lifecycle l ON l.tenant_id=m.tenant_id AND l.message_id=m.id
        WHERE d.state IN ('processed','expired','dead_letter') AND l.payload_purged_at IS NULL
        AND (l.terminal_at IS NULL OR l.terminal_at<=?)
        ORDER BY l.terminal_at,m.tenant_id,m.id LIMIT ?`).all(now - this.limits._terminal_payload_retention_ms, limit) as Row[];
      for (const m of rows) {
        if (m.terminal_at === null) {
          // V1 has no reliable terminal timestamp. Start a conservative grace
          // window when explicitly maintained; never infer or rewrite its history.
          this.recordTerminal(m.tenant_id, m.id, now);
          continue;
        }
        const { _payload, ...header } = JSON.parse(m.envelope) as XNetMessageEnvelope;
        const tombstone = JSON.stringify(header);
        this.run("UPDATE xnet_message SET envelope=?,envelope_bytes=? WHERE tenant_id=? AND id=?",
          tombstone, Buffer.byteLength(tombstone), m.tenant_id, m.id);
        this.options._fault?.("cleanup.after-payload");
        this.run("UPDATE xnet_message_lifecycle SET payload_purged_at=? WHERE tenant_id=? AND message_id=?", now, m.tenant_id, m.id);
      }
      return rows.length;
    });
  }

  private authorizedMessage(g: XNetPrincipalGrant, id: string): Row {
    const m = this.get("SELECT * FROM xnet_message WHERE tenant_id=? AND id=?", g._tenant_id, id);
    if (!m || !((g._source && sourceKey(g._source) === m.source_key) || g._receive_endpoint_ids.includes(m.endpoint_id))) xnetReject("E_XNET_FORBIDDEN");
    return m;
  }
  private statusRow(tenant: string, message: string): XNetMessageStatus {
    const m = this.get("SELECT * FROM xnet_message WHERE tenant_id=? AND id=?", tenant, message)!;
    const d = this.get("SELECT * FROM xnet_delivery WHERE tenant_id=? AND message_id=?", tenant, message)!;
    const attempts = this.connection().prepare(`SELECT id AS _id,delivery_id AS _delivery_id,attempt_no AS _attempt_no,
      registration_id AS _registration_id,epoch AS _owner_epoch,started_at AS _started_at,expires_at AS _claim_expires_at,outcome AS _outcome
      FROM xnet_attempt WHERE tenant_id=? AND delivery_id=? ORDER BY attempt_no`).all(tenant, d.id) as XNetMessageStatus['_attempts'];
    return { _message_id: m.id, _destination: { _tenant_id: tenant, _endpoint_id: m.endpoint_id },
      _state: d.state, _attempt_count: d.attempt_count, _accepted_at: m.accepted_at, _expires_at: m.expires_at,
      _next_attempt_at: d.next_attempt_at, ...(d.receipt_id ? { _receipt_id: d.receipt_id } : {}), _attempts: attempts };
  }
  getStatus(p: XNetPrincipal, message_id: string): XNetMessageStatus {
    assertXNetContract("guid", message_id);
    return this.tx(() => { const g = this.grant(p, "status"); this.authorizedMessage(g, message_id); return this.statusRow(g._tenant_id, message_id); });
  }
}
