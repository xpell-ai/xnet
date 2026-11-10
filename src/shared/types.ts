export type XNetGuid = string;
export const XNET_AVAILABLE_EVENT = "xnet.available";
export interface XNetAvailabilityHint { readonly _registration_id: XNetGuid; readonly _owner_epoch: number }
export type XNetJson = null | boolean | number | string | XNetJson[] | { [key: string]: XNetJson };
export interface XNetAddress { readonly _tenant_id: XNetGuid; readonly _endpoint_id: XNetGuid }
export interface XNetEndpointIdentity {
  readonly _id: XNetGuid;
  readonly _address: XNetAddress;
  readonly _kind: "agent" | "module" | "service";
  readonly _name: string;
  readonly _enabled: boolean;
  readonly _accepted_message_types: readonly string[];
  readonly _processing_contract: "idempotent-v1";
}
export interface XNetEndpointRegistration {
  readonly _id: XNetGuid;
  readonly _endpoint: XNetAddress;
  readonly _principal_id: XNetGuid;
  readonly _runtime_instance_id: XNetGuid;
  // Transport-owned fields are absent in Phase 1A local registrations.
  readonly _connection_id?: string;
  readonly _session_id?: string;
  readonly _owner_instance_id: XNetGuid;
  readonly _owner_epoch: number;
  readonly _registered_at: number;
  readonly _lease_expires_at: number;
}
export type XNetBindingSource =
  | { readonly _kind: "endpoint"; readonly _address: XNetAddress }
  | { readonly _kind: "provider_account"; readonly _adapter_id: string; readonly _account_binding_id: XNetGuid };
export type XNetTrustedSource = XNetBindingSource & { readonly _principal_id: XNetGuid };
export interface XNetRouteBinding {
  readonly _id: XNetGuid;
  readonly _tenant_id: XNetGuid;
  readonly _version: number;
  readonly _source: XNetBindingSource;
  readonly _message_type: string;
  readonly _destination: XNetAddress;
  readonly _enabled: boolean;
  readonly _updated_at: number;
  readonly _updated_by: XNetGuid;
}
export type XNetPayload = {
  readonly _mode: "inline";
  readonly _content_type: "application/json";
  readonly _schema_id: string;
  readonly _data: XNetJson;
};
export interface XNetMessageEnvelope {
  readonly _envelope_version: 1;
  readonly _message_id: XNetGuid;
  readonly _tenant_id: XNetGuid;
  readonly _source: XNetTrustedSource;
  readonly _destination: XNetAddress;
  readonly _message_type: string;
  readonly _correlation_id: XNetGuid;
  readonly _causation_id?: XNetGuid;
  readonly _event_id?: string;
  readonly _dedup_key: string;
  readonly _idempotency_key: string;
  readonly _created_at: number;
  readonly _occurred_at?: number;
  readonly _expires_at: number;
  readonly _route_binding_id: XNetGuid;
  readonly _route_binding_version: number;
  readonly _payload: XNetPayload;
  readonly _metadata: Readonly<{ _trace_id?: string; _content_schema_version?: number }>;
}
export interface XNetSubmitIntent {
  readonly destination?: XNetAddress;
  readonly message_type: string;
  readonly idempotency_key: string;
  readonly event_id?: string;
  readonly correlation_id?: XNetGuid;
  readonly causation_id?: XNetGuid;
  readonly ttl_ms?: number;
  readonly payload: XNetPayload;
}
export interface XNetAcceptance {
  readonly _message_id: XNetGuid;
  readonly _stage: "durably_accepted";
  readonly _duplicate: boolean;
  readonly _accepted_at: number;
  readonly _expires_at: number;
}
export type XNetDeliveryState = "queued" | "leased" | "dispatched" | "retry_wait"
  | "processed" | "expired" | "dead_letter" | "uncertain";
export interface XNetDispatch {
  readonly _envelope: XNetMessageEnvelope;
  readonly _delivery_id: XNetGuid;
  readonly _attempt_id: XNetGuid;
  readonly _registration_id: XNetGuid;
  readonly _owner_epoch: number;
  readonly _claim_token: string;
  readonly _claim_expires_at: number;
}
export interface XNetProcessingAck {
  readonly delivery_id: XNetGuid;
  readonly message_id: XNetGuid;
  readonly attempt_id: XNetGuid;
  readonly registration_id: XNetGuid;
  readonly owner_epoch: number;
  readonly claim_token: string;
  readonly outcome: "processed" | "retryable_failure" | "permanent_failure" | "uncertain";
  readonly receipt_id?: XNetGuid;
  readonly error_code?: "TARGET_RETRYABLE" | "TARGET_PERMANENT" | "TARGET_UNCERTAIN";
}
export interface XNetDeliveryAttempt {
  readonly _id: XNetGuid;
  readonly _delivery_id: XNetGuid;
  readonly _attempt_no: number;
  readonly _registration_id: XNetGuid;
  readonly _owner_epoch: number;
  readonly _started_at: number;
  readonly _claim_expires_at: number;
  readonly _outcome: "leased" | "dispatched" | "processed" | "retryable_failure"
    | "permanent_failure" | "uncertain" | "lease_expired" | "expired";
}
export interface XNetTargetReceipt {
  readonly _id: XNetGuid;
  readonly _tenant_id: XNetGuid;
  readonly _endpoint_id: XNetGuid;
  readonly _message_id: XNetGuid;
  readonly _idempotency_key: string;
  readonly _processed_at: number;
  readonly _outcome: "processed";
}
export interface XNetMessageStatus {
  readonly _message_id: XNetGuid;
  readonly _destination: XNetAddress;
  readonly _state: XNetDeliveryState;
  readonly _attempt_count: number;
  readonly _accepted_at: number;
  readonly _expires_at: number;
  readonly _next_attempt_at: number;
  readonly _receipt_id?: XNetGuid;
  readonly _attempts: readonly XNetDeliveryAttempt[];
}
