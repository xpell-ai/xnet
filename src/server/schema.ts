export const XNET_SCHEMA_VERSION = 2;
export const XNET_APPLICATION_ID = 0x584e4554;

// Owned exclusively by XNet; never installed into XDB or an application database.
export const XNET_SCHEMA_V1 = `
CREATE TABLE xnet_tenant (id TEXT PRIMARY KEY);
CREATE TABLE xnet_endpoint (
  tenant_id TEXT NOT NULL REFERENCES xnet_tenant(id), id TEXT NOT NULL,
  kind TEXT NOT NULL, name TEXT NOT NULL, definition TEXT NOT NULL, epoch INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,kind,name)
);
CREATE TABLE xnet_account (
  tenant_id TEXT NOT NULL REFERENCES xnet_tenant(id), id TEXT NOT NULL,
  adapter_id TEXT NOT NULL, provider TEXT NOT NULL, locator TEXT NOT NULL, definition TEXT NOT NULL,
  PRIMARY KEY(tenant_id,id), UNIQUE(adapter_id,provider,locator)
);
CREATE TABLE xnet_grant (
  tenant_id TEXT NOT NULL REFERENCES xnet_tenant(id), principal_id TEXT NOT NULL,
  definition TEXT NOT NULL, PRIMARY KEY(tenant_id,principal_id)
);
CREATE TABLE xnet_route (
  tenant_id TEXT NOT NULL, id TEXT NOT NULL, source_key TEXT NOT NULL, message_type TEXT NOT NULL,
  endpoint_id TEXT NOT NULL, version INTEGER NOT NULL, definition TEXT NOT NULL,
  PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,source_key,message_type),
  FOREIGN KEY(tenant_id,endpoint_id) REFERENCES xnet_endpoint(tenant_id,id)
);
CREATE TABLE xnet_registration (
  tenant_id TEXT NOT NULL, id TEXT NOT NULL, endpoint_id TEXT NOT NULL, principal_id TEXT NOT NULL,
  instance_id TEXT NOT NULL, epoch INTEGER NOT NULL, expires_at INTEGER NOT NULL, definition TEXT NOT NULL,
  active INTEGER NOT NULL CHECK(active IN (0,1)), PRIMARY KEY(tenant_id,id),
  FOREIGN KEY(tenant_id,endpoint_id) REFERENCES xnet_endpoint(tenant_id,id),
  FOREIGN KEY(tenant_id,principal_id) REFERENCES xnet_grant(tenant_id,principal_id)
);
CREATE TABLE xnet_message (
  tenant_id TEXT NOT NULL, id TEXT NOT NULL, source_key TEXT NOT NULL, dedup_key TEXT NOT NULL,
  idempotency_key TEXT NOT NULL, content_hash TEXT NOT NULL, endpoint_id TEXT NOT NULL,
  accepted_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, envelope_bytes INTEGER NOT NULL, envelope TEXT NOT NULL,
  PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,source_key,dedup_key), UNIQUE(tenant_id,source_key,idempotency_key),
  FOREIGN KEY(tenant_id,endpoint_id) REFERENCES xnet_endpoint(tenant_id,id)
);
CREATE TABLE xnet_delivery (
  tenant_id TEXT NOT NULL, id TEXT NOT NULL, message_id TEXT NOT NULL, endpoint_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('queued','leased','dispatched','retry_wait','processed','expired','dead_letter','uncertain')),
  version INTEGER NOT NULL DEFAULT 0, attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL, claim_expires_at INTEGER, attempt_id TEXT, registration_id TEXT,
  epoch INTEGER, receipt_id TEXT,
  PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,message_id,endpoint_id),
  FOREIGN KEY(tenant_id,message_id) REFERENCES xnet_message(tenant_id,id),
  FOREIGN KEY(tenant_id,endpoint_id) REFERENCES xnet_endpoint(tenant_id,id),
  FOREIGN KEY(tenant_id,registration_id) REFERENCES xnet_registration(tenant_id,id)
);
CREATE TABLE xnet_attempt (
  tenant_id TEXT NOT NULL, id TEXT NOT NULL, delivery_id TEXT NOT NULL, attempt_no INTEGER NOT NULL,
  registration_id TEXT NOT NULL, epoch INTEGER NOT NULL, request_key TEXT NOT NULL,
  nonce TEXT NOT NULL, token_hash TEXT NOT NULL, started_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL, outcome TEXT NOT NULL,
  PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,delivery_id,attempt_no), UNIQUE(tenant_id,registration_id,request_key),
  FOREIGN KEY(tenant_id,delivery_id) REFERENCES xnet_delivery(tenant_id,id),
  FOREIGN KEY(tenant_id,registration_id) REFERENCES xnet_registration(tenant_id,id)
);
CREATE TABLE xnet_ack (
  tenant_id TEXT NOT NULL, attempt_id TEXT NOT NULL, delivery_id TEXT NOT NULL,
  receipt_id TEXT, outcome TEXT NOT NULL, ack_json TEXT NOT NULL, observed_at INTEGER NOT NULL,
  PRIMARY KEY(tenant_id,attempt_id), UNIQUE(tenant_id,receipt_id),
  FOREIGN KEY(tenant_id,attempt_id) REFERENCES xnet_attempt(tenant_id,id),
  FOREIGN KEY(tenant_id,delivery_id) REFERENCES xnet_delivery(tenant_id,id)
);
CREATE INDEX xnet_delivery_due ON xnet_delivery(tenant_id,endpoint_id,state,next_attempt_at,id);
CREATE INDEX xnet_delivery_lease ON xnet_delivery(state,claim_expires_at);
CREATE INDEX xnet_message_expiry ON xnet_message(expires_at);
CREATE UNIQUE INDEX xnet_registration_owner ON xnet_registration(tenant_id,endpoint_id) WHERE active=1;
`;

// Additive migration: historical message/delivery/attempt/ACK rows are untouched.
// Missing lifecycle rows identify V1 hashes and an unknown terminal timestamp.
export const XNET_SCHEMA_V2 = `
CREATE TABLE xnet_message_lifecycle (
  tenant_id TEXT NOT NULL, message_id TEXT NOT NULL,
  hash_version INTEGER NOT NULL CHECK(hash_version IN (1,2)),
  terminal_at INTEGER, payload_purged_at INTEGER,
  PRIMARY KEY(tenant_id,message_id),
  FOREIGN KEY(tenant_id,message_id) REFERENCES xnet_message(tenant_id,id)
);
CREATE INDEX xnet_payload_retention ON xnet_message_lifecycle(terminal_at) WHERE payload_purged_at IS NULL;
`;
