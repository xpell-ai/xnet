export const XNET_LIMITS = Object.freeze({
  _max_envelope_bytes: 64 * 1024, _max_batch: 100,
  _max_queued_per_tenant: 10_000, _max_queued_server: 100_000,
  _max_attempts: 8, _ttl_ms: 86_400_000, _claim_lease_ms: 60_000,
  _registration_lease_ms: 60_000, _retry_base_ms: 1000, _retry_max_ms: 60_000,
  _advance_limit: 100,
  _terminal_payload_retention_ms: 7 * 86_400_000,
});

// A dispatch adds one object level. Its closed scalar fields (three GUIDs,
// a <=128-character token and two safe integers) fit within 2 KiB of JSON,
// including worst-case JSON escaping of every token character.
export const XNET_ENVELOPE_MAX_DEPTH = 16;
export const XNET_DISPATCH_LIMITS = Object.freeze({
  _max_bytes: XNET_LIMITS._max_envelope_bytes + 2048,
  _max_depth: XNET_ENVELOPE_MAX_DEPTH + 1,
  _max_wrapper_bytes: 2048,
});

// Closed schemas used by XNet validation and operation discovery.
export type XNetSchema = {
  type?: "object" | "array" | "string" | "integer" | "boolean";
  properties?: Record<string, XNetSchema>; required?: readonly string[];
  additionalProperties?: false; items?: XNetSchema; minItems?: number; maxItems?: number;
  minLength?: number; maxLength?: number; pattern?: string; minimum?: number;
  const?: string | number | boolean; enum?: readonly string[]; oneOf?: readonly XNetSchema[];
};
const str = (maxLength = 256): XNetSchema => ({ type: "string", minLength: 1, maxLength });
const guid: XNetSchema = { ...str(36), pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" };
const time: XNetSchema = { type: "integer", minimum: 0 };
const positive: XNetSchema = { type: "integer", minimum: 1 };
const bool: XNetSchema = { type: "boolean" };
const literal = (value: string | number): XNetSchema => ({ const: value });
const names: XNetSchema = { ...str(120), pattern: "^[a-z0-9]+(?:[.-][a-z0-9]+)*$" };
export const xnetObjectSchema = (
  properties: Record<string, XNetSchema>, required = Object.keys(properties),
): XNetSchema => ({ type: "object", properties, required, additionalProperties: false });
const address = xnetObjectSchema({ _tenant_id: guid, _endpoint_id: guid });
const source = (trusted: boolean): XNetSchema => ({ oneOf: [
  xnetObjectSchema({ _kind: literal("endpoint"), _address: address, ...(trusted ? { _principal_id: guid } : {}) }),
  xnetObjectSchema({ _kind: literal("provider_account"), _adapter_id: names,
    _account_binding_id: guid, ...(trusted ? { _principal_id: guid } : {}) }),
] });
const payload = xnetObjectSchema({ _mode: literal("inline"), _content_type: literal("application/json"),
  _schema_id: names, _data: {} });
const registration = xnetObjectSchema({ _id: guid, _endpoint: address, _principal_id: guid,
  _runtime_instance_id: guid, _connection_id: str(), _session_id: str(), _owner_instance_id: guid,
  _owner_epoch: positive, _registered_at: time, _lease_expires_at: time },
  ["_id", "_endpoint", "_principal_id", "_runtime_instance_id", "_owner_instance_id", "_owner_epoch", "_registered_at", "_lease_expires_at"]);
const envelope = xnetObjectSchema({
  _envelope_version: literal(1), _message_id: guid, _tenant_id: guid, _source: source(true),
  _destination: address, _message_type: names, _correlation_id: guid, _causation_id: guid,
  _event_id: str(), _dedup_key: str(512), _idempotency_key: str(), _created_at: time,
  _occurred_at: time, _expires_at: time, _route_binding_id: guid, _route_binding_version: positive,
  _payload: payload, _metadata: xnetObjectSchema({ _trace_id: str(64), _content_schema_version: positive }, []),
}, ["_envelope_version", "_message_id", "_tenant_id", "_source", "_destination", "_message_type",
  "_correlation_id", "_dedup_key", "_idempotency_key", "_created_at", "_expires_at",
  "_route_binding_id", "_route_binding_version", "_payload", "_metadata"]);
const acceptance = xnetObjectSchema({ _message_id: guid, _stage: literal("durably_accepted"),
  _duplicate: bool, _accepted_at: time, _expires_at: time });
const attempt = xnetObjectSchema({ _id: guid, _delivery_id: guid, _attempt_no: positive,
  _registration_id: guid, _owner_epoch: positive, _started_at: time, _claim_expires_at: time,
  _outcome: { enum: ["leased", "dispatched", "processed", "retryable_failure", "permanent_failure", "uncertain", "lease_expired", "expired"] } });
const state: XNetSchema = { enum: ["queued", "leased", "dispatched", "retry_wait", "processed", "expired", "dead_letter", "uncertain"] };
export const XNET_SCHEMAS = {
  available: xnetObjectSchema({ _registration_id: guid, _owner_epoch: positive }),
  guid, address, source: source(false), trusted_source: source(true), payload, registration, envelope, acceptance, attempt, state,
  endpoint: xnetObjectSchema({ _id: guid, _address: address, _kind: { enum: ["agent", "module", "service"] },
    _name: { ...names, pattern: "^[a-z0-9]+(?:-[a-z0-9]+)*$" }, _enabled: bool,
    _accepted_message_types: { type: "array", items: names, minItems: 1, maxItems: 100 },
    _processing_contract: literal("idempotent-v1") }),
  route: xnetObjectSchema({ _id: guid, _tenant_id: guid, _version: positive, _source: source(false),
    _message_type: names, _destination: address, _enabled: bool, _updated_at: time, _updated_by: guid }),
  submit: xnetObjectSchema({ destination: address, message_type: names, idempotency_key: str(), event_id: str(),
    correlation_id: guid, causation_id: guid, ttl_ms: positive, payload }, ["message_type", "idempotency_key", "payload"]),
  dispatch: xnetObjectSchema({ _envelope: envelope, _delivery_id: guid, _attempt_id: guid,
    _registration_id: guid, _owner_epoch: positive, _claim_token: str(128), _claim_expires_at: time }),
  ack: xnetObjectSchema({ delivery_id: guid, message_id: guid, attempt_id: guid, registration_id: guid,
    owner_epoch: positive, claim_token: str(128), outcome: { enum: ["processed", "retryable_failure", "permanent_failure", "uncertain"] },
    receipt_id: guid, error_code: { enum: ["TARGET_RETRYABLE", "TARGET_PERMANENT", "TARGET_UNCERTAIN"] } },
    ["delivery_id", "message_id", "attempt_id", "registration_id", "owner_epoch", "claim_token", "outcome"]),
  receipt: xnetObjectSchema({ _id: guid, _tenant_id: guid, _endpoint_id: guid, _message_id: guid,
    _idempotency_key: str(), _processed_at: time, _outcome: literal("processed") }),
  status: xnetObjectSchema({ _message_id: guid, _destination: address, _state: state, _attempt_count: time,
    _accepted_at: time, _expires_at: time, _next_attempt_at: time, _receipt_id: guid,
    _attempts: { type: "array", items: attempt } },
    ["_message_id", "_destination", "_state", "_attempt_count", "_accepted_at", "_expires_at", "_next_attempt_at", "_attempts"]),
};
