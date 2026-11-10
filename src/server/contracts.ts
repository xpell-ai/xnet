import type {
  XNetAcceptance, XNetAddress, XNetBindingSource, XNetDispatch, XNetEndpointIdentity,
  XNetEndpointRegistration, XNetMessageStatus, XNetProcessingAck, XNetRouteBinding, XNetSubmitIntent,
} from "../shared/index.js";
import type { XNET_LIMITS } from "../shared/schemas.js";

/** Trusted host authorization result, never read from command parameters. */
export interface XNetPrincipal {
  readonly _principal_id: string; readonly _tenant_id: string;
  /** Trusted gateway attestation only; never copied from submitted parameters. */
  readonly _connection_id?: string; readonly _session_id?: string; readonly _auth_expires_at?: number;
}
export interface XNetPrincipalGrant extends XNetPrincipal {
  readonly _enabled: boolean;
  readonly _permissions: readonly ("submit" | "receive" | "ack" | "status")[];
  readonly _source?: XNetBindingSource;
  readonly _receive_endpoint_ids: readonly string[];
}
export interface XNetAccountBinding {
  readonly _id: string; readonly _tenant_id: string; readonly _adapter_id: string;
  readonly _provider: string; readonly _account_locator: string; readonly _enabled: boolean;
}
export interface XNetProvisioning {
  readonly tenants: readonly string[];
  readonly endpoints: readonly XNetEndpointIdentity[];
  readonly accounts: readonly XNetAccountBinding[];
  readonly grants: readonly XNetPrincipalGrant[];
  readonly routes: readonly XNetRouteBinding[];
}
export type XNetFaultPoint = "accept.after-message" | "accept.after-delivery"
  | "claim.after-attempt" | "ack.after-receipt" | "cleanup.after-payload";
export interface XNetStoreOptions {
  readonly _db_path: string;
  readonly _claim_key: Uint8Array;
  readonly _clock?: () => number;
  readonly _instance_id?: string;
  readonly _limits?: Partial<typeof XNET_LIMITS>;
  /** Host-only test/failure injection. Not a module operation or serialized config. */
  readonly _fault?: (point: XNetFaultPoint) => void;
}
export interface XNetStore {
  initialize(): void;
  open(): void;
  close(): void;
  isOpen(): boolean;
  provision(input: XNetProvisioning): void;
  setRouteBinding(binding: XNetRouteBinding, expected_version: number): void;
  accept(principal: XNetPrincipal, intent: XNetSubmitIntent): XNetAcceptance;
  acceptBatch(principal: XNetPrincipal, intents: readonly XNetSubmitIntent[]): XNetAcceptance[];
  registerEndpoint(principal: XNetPrincipal, address: XNetAddress): XNetEndpointRegistration;
  renewRegistration(principal: XNetPrincipal, registration_id: string, owner_epoch: number): XNetEndpointRegistration;
  unregisterEndpoint(principal: XNetPrincipal, registration_id: string, owner_epoch: number): void;
  authorizePrincipal(principal: XNetPrincipal, permission: XNetPrincipalGrant['_permissions'][number]): void;
  hasPending(principal: XNetPrincipal, registration_id: string): boolean;
  claimNext(principal: XNetPrincipal, registration_id: string, request_key: string): XNetDispatch | null;
  markDispatched(principal: XNetPrincipal, dispatch: XNetDispatch): void;
  acknowledge(principal: XNetPrincipal, ack: XNetProcessingAck): XNetMessageStatus;
  advance(limit?: number): number;
  /** Host-only bounded maintenance; retains all deduplication and ACK evidence. */
  cleanupTerminalPayloads(limit?: number): number;
  getStatus(principal: XNetPrincipal, message_id: string): XNetMessageStatus;
}
