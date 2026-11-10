import { _xu } from "@xpell/core";
import { XNET_LIMITS, XNET_SCHEMAS, XNET_DISPATCH_LIMITS, XNET_ENVELOPE_MAX_DEPTH, type XNetSchema } from "./schemas.js";
import type {
  XNetAddress, XNetEndpointIdentity, XNetEndpointRegistration, XNetBindingSource,
  XNetRouteBinding, XNetTrustedSource, XNetMessageEnvelope, XNetSubmitIntent,
  XNetAcceptance, XNetDispatch, XNetDeliveryAttempt, XNetProcessingAck, XNetTargetReceipt, XNetDeliveryState,
} from "./types.js";

export class XNetError extends Error {
  constructor(readonly _code: string, message = "XNet request rejected") { super(message); this.name = "XNetError"; }
}
export function xnetReject(code = "E_XNET_INVALID_MESSAGE"): never { throw new XNetError(code); }

// Domain bound on data-only XNet objects, including values hidden by JSON.stringify.
export function assertXNetJson(value: unknown, depth = 0, seen = new Set<object>(), max_depth = XNET_ENVELOPE_MAX_DEPTH): void {
  if (depth > max_depth) xnetReject();
  if (value === null || typeof value === "boolean" || typeof value === "string") return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (typeof value !== "object" || !value || seen.has(value)) xnetReject();
  const proto = Object.getPrototypeOf(value);
  if (!Array.isArray(value) && proto !== Object.prototype && proto !== null) xnetReject();
  seen.add(value);
  if (Reflect.ownKeys(value).some(k => typeof k !== "string")) xnetReject();
  for (const key of Object.getOwnPropertyNames(value)) {
    if (Array.isArray(value) && key === "length") continue;
    const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
    if (!descriptor.enumerable || !('value' in descriptor) || ["__proto__", "constructor", "prototype"].includes(key)) xnetReject();
    assertXNetJson(descriptor.value, depth + 1, seen, max_depth);
  }
  if (Array.isArray(value) && (Object.keys(value).length !== value.length
    || Object.keys(value).some((key, i) => key !== String(i)))) xnetReject();
  seen.delete(value);
}

// Validates only the closed schema vocabulary emitted by this package.
function matches(s: XNetSchema, v: unknown): boolean {
  if (s.oneOf) return s.oneOf.filter(item => matches(item, v)).length === 1;
  if (s.const !== undefined && v !== s.const) return false;
  if (s.enum && (typeof v !== "string" || !s.enum.includes(v))) return false;
  if (s.type === "string") return typeof v === "string" && v.length >= (s.minLength ?? 0)
    && v.length <= (s.maxLength ?? Infinity) && (!s.pattern || new RegExp(s.pattern, 'u').test(v));
  if (s.type === "integer") return typeof v === "number" && Number.isSafeInteger(v) && v >= (s.minimum ?? 0);
  if (s.type === "boolean") return typeof v === "boolean";
  if (s.type === "array") return Array.isArray(v) && v.length >= (s.minItems ?? 0)
    && v.length <= (s.maxItems ?? Infinity) && v.every(item => !s.items || matches(s.items, item));
  if (s.type === "object") {
    if (!_xu.is_plain_object(v)) return false;
    return (s.required ?? []).every(key => Object.hasOwn(v, key))
      && Object.entries(v).every(([key, item]) => Object.hasOwn(s.properties ?? {}, key) && matches(s.properties![key], item));
  }
  return true;
}
export function assertXNetContract(name: keyof typeof XNET_SCHEMAS, value: unknown): void {
  if (name === "dispatch") { assertXNetDispatch(value); return; }
  assertXNetSchema(XNET_SCHEMAS[name], value);
}
/** Validate package-owned operation schemas; callers never select a schema through commands. */
export function assertXNetSchema(schema: XNetSchema, value: unknown,
  limits = { _max_bytes: XNET_LIMITS._max_envelope_bytes, _max_depth: XNET_ENVELOPE_MAX_DEPTH }): void {
  assertXNetJson(value, 0, new Set(), limits._max_depth);
  if (new TextEncoder().encode(JSON.stringify(value)).byteLength > limits._max_bytes) xnetReject("E_XNET_TOO_LARGE");
  if (!matches(schema, value)) xnetReject();
}
export function assertXNetAddress(v: unknown): asserts v is XNetAddress { assertXNetContract("address", v); }
export function assertXNetEndpointIdentity(v: unknown): asserts v is XNetEndpointIdentity {
  assertXNetContract("endpoint", v);
  const e = v as XNetEndpointIdentity;
  if (e._id !== e._address._endpoint_id || new Set(e._accepted_message_types).size !== e._accepted_message_types.length) xnetReject();
}
export function assertXNetRegistration(v: unknown): asserts v is XNetEndpointRegistration {
  assertXNetContract("registration", v);
  const r = v as XNetEndpointRegistration;
  if (r._lease_expires_at <= r._registered_at) xnetReject();
}
export function assertXNetBindingSource(v: unknown): asserts v is XNetBindingSource { assertXNetContract("source", v); }
export function assertXNetTrustedSource(v: unknown): asserts v is XNetTrustedSource { assertXNetContract("trusted_source", v); }
export function assertXNetRouteBinding(v: unknown): asserts v is XNetRouteBinding {
  assertXNetContract("route", v);
  const r = v as XNetRouteBinding;
  if (r._tenant_id !== r._destination._tenant_id || (r._source._kind === "endpoint" && r._source._address._tenant_id !== r._tenant_id)) xnetReject("E_XNET_FORBIDDEN");
}
export function assertXNetSubmitIntent(v: unknown): asserts v is XNetSubmitIntent {
  assertXNetContract("submit", v);
  // TTL policy belongs to the configured store; this portable contract checks shape.
}
export function assertXNetEnvelope(v: unknown): asserts v is XNetMessageEnvelope {
  assertXNetContract("envelope", v);
  const e = v as XNetMessageEnvelope;
  if (e._tenant_id !== e._destination._tenant_id || e._expires_at <= e._created_at
    || (e._source._kind === "endpoint" && e._source._address._tenant_id !== e._tenant_id)) xnetReject();
}
export function assertXNetAcceptance(v: unknown): asserts v is XNetAcceptance { assertXNetContract("acceptance", v); }
export function assertXNetDispatch(v: unknown): asserts v is XNetDispatch {
  assertXNetSchema(XNET_SCHEMAS.dispatch, v, XNET_DISPATCH_LIMITS);
  assertXNetEnvelope((v as XNetDispatch)._envelope);
  const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
  if (bytes(v) - bytes((v as XNetDispatch)._envelope) > XNET_DISPATCH_LIMITS._max_wrapper_bytes) xnetReject("E_XNET_TOO_LARGE");
}
export function assertXNetAttempt(v: unknown): asserts v is XNetDeliveryAttempt { assertXNetContract("attempt", v); }
export function assertXNetDeliveryState(v: unknown): asserts v is XNetDeliveryState { assertXNetContract("state", v); }
export function assertXNetProcessingAck(v: unknown): asserts v is XNetProcessingAck {
  assertXNetContract("ack", v);
  const a = v as XNetProcessingAck;
  if ((a.outcome === "processed") !== (a.receipt_id !== undefined)) xnetReject();
  if (a.error_code && a.error_code !== ({ retryable_failure: "TARGET_RETRYABLE", permanent_failure: "TARGET_PERMANENT", uncertain: "TARGET_UNCERTAIN", processed: "" })[a.outcome]) xnetReject();
}
export function assertXNetTargetReceipt(v: unknown): asserts v is XNetTargetReceipt { assertXNetContract("receipt", v); }
