import { XModule, XCommand, XError, XResponseError, XResponseOK, type XCommandData,
  type XpellSkill, type XpellSkillCommand } from "@xpell/core";
import { XNET_SCHEMAS, XNET_ENVELOPE_MAX_DEPTH, XNetError, assertXNetJson, assertXNetSchema, xnetReject, xnetObjectSchema } from "../shared/index.js";
import type { XNetAddress, XNetEndpointRegistration, XNetProcessingAck, XNetSubmitIntent } from "../shared/index.js";
import type { XNetPrincipal, XNetStore } from "./contracts.js";

const defs = {
  "get-readiness": { schema: xnetObjectSchema({}), output: { type: "object" }, description: "Read safe local readiness.", effects: "none" },
  submit: { schema: XNET_SCHEMAS.submit, output: XNET_SCHEMAS.acceptance, description: "Commit an authorized logical message.", effects: "durable acceptance" },
  "register-endpoint": { schema: xnetObjectSchema({ address: XNET_SCHEMAS.address }), output: XNET_SCHEMAS.registration, description: "Register a provisioned local endpoint.", effects: "owner epoch and lease" },
  "renew-registration": { schema: xnetObjectSchema({ registration_id: XNET_SCHEMAS.guid, owner_epoch: { type: "integer", minimum: 1 } }),
    output: XNET_SCHEMAS.registration, description: "Renew the current endpoint owner without extending claims.", effects: "registration lease" },
  "unregister-endpoint": { schema: xnetObjectSchema({ registration_id: XNET_SCHEMAS.guid, owner_epoch: { type: "integer", minimum: 1 } }),
    output: xnetObjectSchema({ _stopped: { const: true } }), description: "Release the current endpoint registration.", effects: "registration fencing" },
  "receive-next": { schema: xnetObjectSchema({ registration_id: XNET_SCHEMAS.guid,
    request_key: { type: "string", minLength: 1, maxLength: 256 } }), output: { oneOf: [XNET_SCHEMAS.dispatch,
      xnetObjectSchema({ _empty: { const: true } })] }, description: "Claim and mark one local dispatch.", effects: "claim and attempt" },
  "ack-processed": { schema: XNET_SCHEMAS.ack, output: XNET_SCHEMAS.status, description: "Commit the target processing outcome.", effects: "durable acknowledgment" },
  "get-message-status": { schema: xnetObjectSchema({ message_id: XNET_SCHEMAS.guid }), output: XNET_SCHEMAS.status, description: "Read authorized status without message content.", effects: "none" },
} satisfies Record<string, { schema: unknown; output: unknown; description: string; effects: string }>;

export const XNET_OPS: Record<string, XpellSkillCommand & Record<string, unknown>> = Object.fromEntries(
  Object.entries(defs).map(([op, def]) => [op, {
    _id: op, _name: op, _scope: "server", _description: def.description,
    _input_schema: def.schema, _output_schema: def.output,
    _permissions: [`xnet:${op}`], _side_effects: def.effects, _examples: [],
    _errors: ["E_XNET_FORBIDDEN", "E_XNET_INVALID_MESSAGE", "E_XNET_NOT_READY", "E_XNET_STORE_UNAVAILABLE", "E_XNET_STALE_CLAIM", "E_XNET_IDEMPOTENCY_CONFLICT", "E_XNET_OWNER_CONFLICT", "E_XNET_CAPACITY", "E_XNET_TOO_LARGE"],
  }]),
);
export const XNET_SKILL: XpellSkill & Record<string, unknown> = {
  _id: "xnet", _name: "xnet", _title: "XNet durable core", _type: "server-module-api",
  _version: "0.2.0", _scope: "server", _package: "@xpell/xnet",
  _description: "Tenant-scoped transactional mailbox with optional authenticated Wormholes host composition.",
  _runtime: { _target: "server", _hosts: ["xnode"] }, _ops: XNET_OPS,
  _config: [{ _name: "store", _required: true }, { _name: "authorize", _required: true }],
  _secrets: [{ _name: "claim_key", _owner: "host-owned SQLite store; never metadata values" }],
  _permissions: Object.keys(XNET_OPS).map(op => `xnet:${op}`),
  _readiness: { _checks: ["store", "authorize", "explicit_start"] }, _examples: [],
  _exports: { _modules: [{ _name: "xnet", _scope: "server", _ops: Object.values(XNET_OPS) }] },
};

export interface XNetModuleOptions {
  readonly _store?: XNetStore;
  /** Host authority, not a verifier of caller-supplied _auth/_ctx fields. */
  readonly _authorize?: (command: Readonly<XCommandData>) => XNetPrincipal | undefined;
  readonly _advance_on_frame?: boolean;
  /** Trusted host callbacks. Transport objects never enter command parameters. */
  readonly _on_registration?: (principal: XNetPrincipal, registration: XNetEndpointRegistration) => void;
  readonly _frame_work?: () => Promise<void>;
}
export class XNetModule extends XModule {
  static _name = "xnet";
  static _ops = XNET_OPS;
  static _skill = XNET_SKILL;
  private readonly options: XNetModuleOptions;
  private readonly contexts = new WeakMap<object, XNetPrincipal>();
  private running = false;
  private advancing = false;
  private frameError?: string;
  private frameBusy = false;

  constructor(options: XNetModuleOptions = {}) { super({ _name: "xnet" }); this.options = { ...options }; }
  start(): void {
    if (!this.options._store || !this.options._authorize) xnetReject("E_XNET_NOT_READY");
    this.options._store.open(); this.running = true; this.frameError = undefined;
  }
  stop(): void { this.running = false; this.options._store?.close(); }
  get_readiness() {
    return { _ready: this.running && this.options._store?.isOpen() === true,
      _status: this.running && this.options._store?.isOpen() ? "ready" : "not_ready",
      _missing_config: [!this.options._store && "store", !this.options._authorize && "authorize"].filter(Boolean),
      ...(this.frameError ? { _error_code: this.frameError } : {}) };
  }
  advance(): number {
    if (!this.running || !this.options._store) xnetReject("E_XNET_NOT_READY");
    if (this.advancing) return 0;
    this.advancing = true;
    try { return this.options._store.advance(); } finally { this.advancing = false; }
  }
  override async onFrame(frame_number: number): Promise<void> {
    if (this.frameBusy) return;
    this.frameBusy = true;
    try {
      await super.onFrame(frame_number);
      if (this.running && this.options._advance_on_frame) {
        this.advance(); await this.options._frame_work?.();
      }
    } catch (e) { this.frameError = e instanceof XNetError ? e._code : "E_XNET_STORE_UNAVAILABLE"; this.running = false; }
    finally { this.frameBusy = false; }
  }
  override async execute(command: XCommand | XCommandData) {
    // Core applies _output even to returned failure responses. Reject by throwing
    // before dispatch so a forbidden command cannot write caller-chosen XData.
    if (command?._output) throw new XError("E_XNET_FORBIDDEN", "XNet does not accept output routing");
    try {
      const data = command instanceof XCommand
        ? { _module: command._module, _op: command._op, _params: command._params ?? {} } : command;
      // The command adds one level around its independently bounded parameters.
      assertXNetJson(data, 0, new Set(), XNET_ENVELOPE_MAX_DEPTH + 1);
      if (command._module !== "xnet" || !Object.hasOwn(XNET_OPS, command._op)
        || command._object !== undefined
        || Object.keys(command).some(k => !(command instanceof XCommand
          ? ["_module", "_op", "_params", "d", "_output", "_object"] : ["_module", "_op", "_params"]).includes(k))) xnetReject("E_XNET_FORBIDDEN");
      assertXNetSchema(defs[command._op as keyof typeof defs].schema, command._params ?? {});
      const principal = this.options._authorize?.(command);
      if (!principal) xnetReject("E_XNET_FORBIDDEN");
      this.contexts.set(command, { ...principal });
      return await super.execute(command);
    } catch (e) {
      const code = e instanceof XNetError ? e._code : "E_XNET_STORE_UNAVAILABLE";
      return new XResponseError(new XError(code, "XNet operation rejected")).toXData();
    } finally { this.contexts.delete(command); }
  }
  private context(cmd: XCommand): { store: XNetStore; principal: XNetPrincipal } {
    const principal = this.contexts.get(cmd);
    if (!principal) xnetReject("E_XNET_FORBIDDEN");
    if (!this.running || !this.options._store) xnetReject("E_XNET_NOT_READY");
    return { store: this.options._store, principal };
  }
  async _get_readiness(cmd: XCommand) {
    if (!this.contexts.has(cmd)) xnetReject("E_XNET_FORBIDDEN");
    if (Object.keys(cmd._params ?? {}).length) xnetReject();
    return new XResponseOK(this.get_readiness()).toXData();
  }
  async _submit(cmd: XCommand) {
    const { store, principal } = this.context(cmd);
    return new XResponseOK(store.accept(principal, cmd._params as unknown as XNetSubmitIntent)).toXData();
  }
  async _register_endpoint(cmd: XCommand) {
    const { store, principal } = this.context(cmd);
    const p = cmd._params ?? {};
    if (Object.keys(p).length !== 1 || !Object.hasOwn(p, "address")) xnetReject();
    const registration = store.registerEndpoint(principal, p.address as XNetAddress);
    this.options._on_registration?.(principal, registration);
    return new XResponseOK(registration).toXData();
  }
  async _renew_registration(cmd: XCommand) {
    const { store, principal } = this.context(cmd); const p = cmd._params!;
    const registration = store.renewRegistration(principal, p.registration_id as string, p.owner_epoch as number);
    this.options._on_registration?.(principal, registration);
    return new XResponseOK(registration).toXData();
  }
  async _unregister_endpoint(cmd: XCommand) {
    const { store, principal } = this.context(cmd); const p = cmd._params!;
    store.unregisterEndpoint(principal, p.registration_id as string, p.owner_epoch as number);
    return new XResponseOK({ _stopped: true }).toXData();
  }
  async _receive_next(cmd: XCommand) {
    const { store, principal } = this.context(cmd);
    const p = cmd._params ?? {};
    if (Object.keys(p).length !== 2 || !Object.hasOwn(p, "registration_id") || !Object.hasOwn(p, "request_key")) xnetReject();
    const dispatch = store.claimNext(principal, p.registration_id as string, p.request_key as string);
    if (dispatch) store.markDispatched(principal, dispatch);
    return new XResponseOK(dispatch ?? { _empty: true }).toXData();
  }
  async _ack_processed(cmd: XCommand) {
    const { store, principal } = this.context(cmd);
    return new XResponseOK(store.acknowledge(principal, cmd._params as unknown as XNetProcessingAck)).toXData();
  }
  async _get_message_status(cmd: XCommand) {
    const { store, principal } = this.context(cmd); const p = cmd._params ?? {};
    if (Object.keys(p).length !== 1 || !Object.hasOwn(p, "message_id")) xnetReject();
    return new XResponseOK(store.getStatus(principal, p.message_id as string)).toXData();
  }
}
export function createXNetModule(options: XNetModuleOptions = {}): XNetModule { return new XNetModule(options); }
