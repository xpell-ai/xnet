# @xpell/xnet — Phase 1B

Tenant-scoped durable messaging with an authenticated, Node-only Wormholes endpoint. Architecture decisions D1–D10 are approved in [HLD Section 14](https://docs.google.com/document/d/1YxqZkS3ZERDJX_bh30PkqQRGoyL07ja8FO0CoTME8EU/edit). The separate Phase 1B implementation authorization and evidence are recorded in [LLD section 16](../../docs/xnet-technical-design-v0.1.md#16-phase-1b-wormholes-implementation-record).

Phase 1B preserves the accepted Phase 1A.1 store and adds public Wormholes v2 composition. No provider, XAgent, Pulse, XNode boot change, scheduler, or HA is included. Importing this package registers no modules, opens no database, starts no engine, and connects to no service.

## Exports and dependencies

| Export | Contents |
| --- | --- |
| `@xpell/xnet` | Portable DTOs, closed JSON schemas, validators, safe XNet errors/defaults |
| `@xpell/xnet/shared` | Same portable contracts |
| `@xpell/xnet/server` | Store/module APIs, `XNetWormholesHost`, `createXNetWormholesHost`, host/credential types |
| `@xpell/xnet/client` | Node-only `XNetClient`, `createXNetClient`, options/result types, `XNET_CLIENT_OPS`, `XNET_CLIENT_SKILL` |

The only direct runtime dependency is `better-sqlite3`; Core and Node `^2.0.7` are peers. Root/shared stay platform-neutral; client/server use public `@xpell/node` exports. Client does not import the SQLite store or server adapter. Compatibility was verified against the current workspace builds, not every historical Node release. No Agent/Pulse/MCP/provider dependency exists. Version `0.2.0` marks the newly implemented client surface; the message envelope remains version 1.

## Initialization and schema

Supply an **absolute, isolated** database path whose parent exists and a host-owned claim key of at least 32 bytes. Do not point it at XDB or a product database. Construction performs no IO. Call `initialize()` explicitly to create schema version 2, or `open()` to reopen an initialized database. Neither commands nor normal reads/writes run migrations.

`initialize()` transactionally installs **0 → 1 → 2**, or explicitly upgrades an owned **1 → 2** development store. `open()` rejects version 1 until that explicit upgrade. Before upgrading existing development storage, take and verify a consistent snapshot using the previous version's backup API, then upgrade an isolated copy. Version 2 adds only `xnet_message_lifecycle`: it does not rewrite historical message, delivery, registration, attempt or ACK rows. Foreign databases and unsupported versions are rejected before write PRAGMAs. No production or product-store migration is authorized by this package.

Eleven owned tables hold tenants, endpoints, account bindings, principal grants, routes, registrations, messages, deliveries, attempts, acknowledgments, and message lifecycle metadata. The last table records semantic-hash version, terminal time and payload cleanup time. Every canonical ID is a GUID; epochs/counters are not identities. Composite tenant foreign keys isolate referenced records. Unique keys enforce source-scoped deduplication, intent identity, one active endpoint owner, one delivery/message, and idempotent attempts/ACK receipts.

SQLite uses WAL, `synchronous=FULL`, foreign keys, a bounded 1s busy timeout, and short immediate write transactions. Acceptance commits envelope and queued delivery together; claim commits attempt plus version-fenced ownership together; ACK commits receipt and resulting state together. Exceptions roll back the unit of work and expose safe errors. Store ownership uses SQLite `application_id` and `user_version`; no XDB private handles or schema are accessed.

`backup(new_absolute_path)` uses SQLite's consistent backup API in a newly created private directory beside the destination, syncs the completed snapshot, then atomically publishes it with a non-replacing hard link and syncs the parent directory. A raced file/directory/symlink is never overwritten. Temporary artifacts are removed on success or failure. The destination filesystem must support hard links; unsupported operations fail safely. Restore by opening the resulting separate file with a new store instance. Tests verify processed state and deduplication after restore. WAL/FULL and process-crash tests establish local transaction behavior, not hardware/disk-loss protection or HA.

## Trusted local composition

Provision tenants, exact endpoints, accounts, grants, and routes through `store.provision(trusted_configuration)`. This host-only method commits the complete configuration atomically and is not a command. Host-only `setRouteBinding(binding, expected_version)` performs a versioned route update. There is no public provisioning/network admin API.

Each trusted principal references a stored tenant/grant. Submitted intents cannot supply authoritative tenant, source, session, connection, or binding. The stored grant selects source; exact source/type binding selects destination. An endpoint caller may request only its binding's exact same-tenant destination. Provider-account principals cannot choose a destination. Account locators are linkage, not credentials. Provider verification remains a host responsibility; transport credentials use the explicit verifier below.

```ts
import { _x } from '@xpell/core';
import { SQLiteXNetStore, createXNetModule } from '@xpell/xnet/server';

// All four values come from trusted host composition, not command parameters.
const store = new SQLiteXNetStore({ _db_path: isolatedAbsolutePath, _claim_key: hostClaimKey });
store.initialize(); // First initialization only; use open() for existing schema.
store.provision(trustedConfiguration); // First provisioning, not on every restart.

const authority = new WeakMap();
const module = createXNetModule({ _store: store, _authorize: command => authority.get(command) });
await _x.loadModuleAsync(module);
module.start();
const command = { _module: 'xnet', _op: 'submit', _params: validatedIntent };
authority.set(command, trustedPrincipal);
const result = await _x.execute(command);
module.stop(); // Explicit bounded work is synchronous; closes this module's store.
```

`_authorize` must be a trusted capability lookup such as the example; do not implement it by reading caller `_auth`, `_ctx`, tenant, or principal fields. Module methods require the attestation associated with the original command object. Stored grants are rechecked for each operation. The direct store API is a host/service API, not a safe raw JSON RPC boundary.

Public commands are `get-readiness`, `submit`, `register-endpoint`, `renew-registration`, `unregister-endpoint`, `receive-next`, `ack-processed`, and `get-message-status`. Their schemas, permissions, scope, side effects, and errors live in one keyed `XNET_OPS` record and canonical `XNET_SKILL`. Discovery does not grant permission or expose MCP tools. JSON commands and existing `XCommand` instances are supported. Unknown operations, object routing, output routing, extra authority params, non-JSON data, unsupported versions, and oversized envelopes are rejected. Most operation failures return canonical `XResponseData` errors. Forbidden `_output` throws `XError` before dispatch because current Core applies output routing even to returned failure responses; the caller should handle that rejection.

## Authenticated Wormholes host

`createXNetWormholesHost({ _store, _verify_credential, _clock? })` owns the original single `XNetModule` and canonical gateways, not an HTTP listener or another XModule. The bounded host verifier returns `{ _principal: { _principal_id, _tenant_id }, _expires_at }` or `undefined`. Credentials are supplied through trusted composition; no issuer or production credential store is included. Use the same clock for host/store/client in deterministic tests.

```ts
import { XNode } from '@xpell/node';
import { createXNetWormholesHost } from '@xpell/xnet/server';

const host = createXNetWormholesHost({ _store: store, _verify_credential: verifyCredential });
const node = new XNode();
await node.start({ ...host.nodeOptions(), _work_folder: isolatedWorkFolder });
const server = node._web_server._secured_web_server ?? node._web_server._web_server;
if (!server) throw new Error('Host server unavailable');
await host.start(server);
// Existing engine frames drive host._module; do not also install another driver.
// Shutdown: await host.stop(), then explicitly stop host-owned listeners/engine/services.
```

This is composition guidance, not a deployment script. `nodeOptions()` supplies `_modules`, ordered canonical REST routes, and `_web_settings["enable-wormhole"] = false` to disable the permissive default gateway. `start(server)` attaches the canonical `/wh/v2` WS server with mandatory AUTH, inbound EVT disabled, 128 KiB input, and connect/message logging disabled. It refuses an existing upgrade listener or a different loaded `xnet` module. The adapter secures its Wormholes path; the application owns HTTP/TLS exposure and other routes. REST authentication/calls deliberately fail closed because this implementation requires a live server-owned WS connection; canonical hello remains available.

Every REQ revalidates the connection's credential, stable principal, server session and stored grant before the exact eight-operation allowlist. Register/renew/receive also require ACK permission. A private one-use command-object attestation carries authority to the module. Serialized `_ctx`, `_auth`, `_wid`, `_sid`, `_from` and `_to` never supply authority; gateway metadata is stripped before closed command validation. Payload identity/command fields remain ordinary data. Credentials and claim tokens are not logged by XNet.

`start`, `stop`, `advance`, `nodeOptions`, and `_module` are the host surface. Frame maintenance visits at most `_frame_limit` sessions for cleanup and at most that many for hints, with separate fair cursors; defaults are 100, maximum 1000. `_max_connections` defaults to 1000, maximum 10,000. Pending work produces repeated `xnet.available` hints no faster than `_hint_interval_ms` (default 1000). No payload or claim is sent in a hint. A lost hint is recovered by a later explicit frame. Stop terminates owned sockets, releases valid registrations and closes the module/store; it does not stop XNode's HTTP listeners, Core engine or other services. Core has no module-unload API; restart uses a fresh process or the same adapter, not hot replacement by a second module object.

## Node endpoint client

```ts
import { _x } from '@xpell/core';
import { createXNetClient } from '@xpell/xnet/client';

const client = createXNetClient({
  _url: 'ws://127.0.0.1:3400/wh/v2',
  _address: provisionedAddress,
  _credential: loadCredential,
  _handlers: { 'app.event.v1': { _module: 'my-consumer', _op: 'process-message' } },
});
await _x.loadModuleAsync(client);
await client.start();
// Drive through existing XModule frames OR await client.advance() explicitly.
client.stop();
```

`XNetClient` extends `XModule` (`xnet-client`). Its API is `start(): Promise<void>`, `stop(): void`, `advance(): Promise<void>`, `onFrame(frame)`, `get_readiness()` and `get_registration()` (a safe clone). The sole local command is `get-readiness`. The client opens only an outbound socket. It authenticates, registers its fixed logical address, renews ownership, drains bounded work, invokes the fixed handler through `_x.execute`, and ACKs the result. `_execute` may inject an equivalent trusted executor; `_clock` injects lifecycle time. There is no browser client or caller-selected command path.

Each handler receives `{ envelope }` and returns canonical `XResponseOK` containing either a durable `XNetTargetReceipt`, or exactly `{ _outcome: 'retryable_failure' | 'permanent_failure' | 'uncertain' }`. Receipt tenant, endpoint, Message ID and idempotency key must match the dispatch. A missing handler is permanent failure; exceptions, failed/malformed responses or mismatched receipts become uncertain. The target must atomically commit its effect and receipt, or use an outbox/external idempotency policy. Handlers and credential verifiers must complete within their ownership deadline; arbitrary application side effects cannot be forcibly cancelled by this client.

`_max_drain` defaults to 4 (maximum 100); a single guarded pass cannot overlap another. `_max_reconnect_attempts` defaults to 8 (maximum 100), with 1s base/60s capped exponential backoff, driven by explicit frames. Successful registration resets the reconnect budget. Connect/call deadlines default to 10s and are capped at 30s. Live-connection request timeouts preserve the receive key or pending ACK; three consecutive RPC failures reconnect with fresh AUTH. Connection replacement drops old claim capabilities; target receipt replay prevents a second committed effect. No arbitrary uncertain outcome is automatically retried.

**Accepted transport timing contract:** XNet adds no timers. Public `WHWSClient` itself starts a one-second request-timeout sweep and a connect-deadline timer using real time; it has no public injected-clock/frame interface. These existing transport timers are architecturally permitted, with `_auto_reconnect` explicitly disabled because it is unimplemented. XNet's own draining, renewal, retries, hint cadence and reconnect backoff use the injected clock and frames. Stop cancels XNet's pending lifecycle waits and closes the transport; it cannot cancel arbitrary handler work, and the SDK's connect timer may persist until its configured deadline.

## Delivery and deterministic driver

`registerEndpoint(principal, address)` creates a registration with a durable per-endpoint epoch. Trusted local callers retain the Phase 1A behavior without physical IDs. Gateway registrations additionally bind server-owned connection/session GUIDs. One host process owns the mailbox. Multiple SQLite connections used by that host must share the same explicit `_instance_id` and claim key. A fresh host instance fences old registrations; multi-node takeover/ownership is not supported.

`claimNext(principal, registration_id, request_key)` returns one lease or `null`. Reusing a live request key returns the same active attempt/token. A second independent request receives no extra credit while the endpoint has an active claim. `markDispatched` durably records dispatch initiation; the module's `receive-next` performs both steps. This does not establish transport receipt or business processing.

The advertised claim deadline is the minimum of the configured claim lease, message expiry, registration expiry and credential expiry. ACK is valid strictly before that deadline while the same grant/owner remains authorized. Re-registering the same live principal/connection returns its unchanged registration; competing connections are rejected. `renewRegistration` preserves ID/epoch and cannot extend an existing claim. Renewal is capped by the authenticated credential deadline. The client renews after roughly one third of the remaining lease. Disconnect cleanup releases still-authorized ownership; otherwise expiry fences it. Expired/replaced registrations reject old attempts. Historical replayed claims also advertise the shorter ownership deadline without rewriting stored attempts.

ACK supplies message/delivery/attempt/registration IDs, epoch, and claim token. State, tenant, stored grant, current registration, lease and token hash must match. Processing requires a receipt GUID. ACK replay with the same receipt is idempotent even after the processing deadline if the registration remains valid; conflicting outcomes and stale claims fail. Tokens are HMAC-derived from a random persisted attempt nonce and ownership fields; only hashes/nonces persist. Phase 1A uses one host-provided key, without a rotation/keyring API.

States are `queued → leased → dispatched → processed`, with explicit `retry_wait`, `expired`, `dead_letter`, and `uncertain` branches. Default TTL is 24h, claim/registration leases 60s, max attempts 8; deterministic exponential retry starts at 1s and caps at 60s. Limits and the clock are injectable. No jitter or exact-time SLA is claimed. Offline queued work consumes no attempts. `uncertain` is parked and never automatically retried, even after TTL; reconciliation/redrive is deferred.

`_limits._ttl_ms` is both the default and maximum for new acceptances. An omitted producer TTL stays omitted in the version-2 intent hash; an explicit TTL is hashed exactly. Explicit values above the configured maximum are rejected, never clamped. Duplicate lookup precedes current route/capacity/TTL policy, so unchanged existing events retain their original ID, destination and expiry across configuration changes. Legacy hashes compare against the stored original duration; version 1 did not distinguish omitted TTL from an explicit equal value.

Envelopes remain bounded to 64 KiB and depth 16 (including the envelope root); a host may lower the byte limit. Dispatch validation permits one additional wrapper level and 2 KiB of bounded wrapper overhead, including worst-case token escaping, while independently retaining the envelope limits. Canonical command validation similarly allows its parameter wrapper. Oversized or overdeep new messages fail before acceptance.

Call `module.advance()` or `store.advance()` from a deterministic driver after advancing the injected clock. A pass handles at most the configured `_advance_limit` (default 100). It expires work, converts expired leases to retry eligibility, and makes due retries queued. Retry due times persist; the durable core adds no timers or scheduler. The optional `_advance_on_frame: true` uses XModule frames without overlapping work; it is off by default for local composition and enabled by the Wormholes host. Frame errors stop readiness until explicit host recovery.

Consumers own their durable processing ledger and must atomically protect effects plus receipt, or use an outbox/external idempotency policy. XNet records an asserted processed receipt; it cannot atomically commit a different consumer/provider store. The tests simulate a separate durable target ledger and prove one effect when an ACK is lost. Provider acceptance and recipient delivery remain different future observations. This is at-least-once delivery, not distributed exactly-once effects.

## Explicit terminal payload cleanup

The host calls `store.cleanupTerminalPayloads(limit?)` separately from delivery advancement. A pass processes at most `_advance_limit` rows (default 100); the returned count includes legacy grace-window initialization and payload redactions. No timer, network operation or automatic database-open cleanup runs. `_terminal_payload_retention_ms` defaults to seven days and accepts positive values up to seven days. Eligibility starts at the transaction that records `processed`, `expired` or `dead_letter`.

Cleanup atomically removes only the envelope's `_payload` object and records the purge time. Envelope headers, immutable message IDs/destinations, semantic hashes, dedup keys, delivery/attempt history, and ACK/receipt evidence remain as durable tombstones. They are retained indefinitely, satisfying the minimum 30-day/replay-horizon evidence requirement. Status and matching duplicate acceptance continue to work. Queued, leased, dispatched, retry-wait and uncertain messages are never cleaned by this method, even past TTL. There is no terminal redrive/reset operation.

Version-1 terminal rows lack a reliable terminal timestamp. Their first explicit cleanup pass records a conservative grace-window start without rewriting historical rows or deleting payload; a later due pass can redact them. Hosts must schedule explicit maintenance passes to meet retention deadlines. This is logical row cleanup, not secure erasure of old SQLite pages, WAL files or independent backups.

## Validation

Use Node 22 with the matching native SQLite dependency. From the workspace's `packages/` directory:

```sh
node --version
pnpm -C modules/xnet typecheck
pnpm -C modules/xnet build
pnpm -C modules/xnet test
```

The original 22 core tests and 16 reliability regressions remain. Six ownership/lifecycle tests and twelve transport tests add real canonical Wormholes traffic between an isolated host process and two outbound simulated endpoint processes, with separate durable target ledgers. Tests use temporary SQLite files, ephemeral `127.0.0.1` ports, two synthetic tenants/phone IDs, mocked credential/provider verification, injected lifecycle time, response/hint loss and owned-process crashes. No real provider, existing service or product data is used. The fixture's HTTP server exists only to bind exact loopback without starting unrelated XNode services; production composition uses the public XNode hooks above.

After a build, run focused suites with `node --test modules/xnet/tests/ownership.test.mjs modules/xnet/tests/transport.test.mjs`, or the original regressions with `node --test modules/xnet/tests/contracts.test.mjs modules/xnet/tests/core.test.mjs modules/xnet/tests/reliability.test.mjs`. Transport tests require permission to listen on ephemeral loopback ports. They retain the existing SDK's real-time request deadlines while all XNet business advancement uses injected time.

## Review gate and remaining work

Phase 1A is accepted and Phase 1B is architecturally accepted, including the exclusive XNode/Core ownership and owned shutdown correction. Phase 1C's approved mock-only gateway is implemented in private tests and awaits explicit acceptance. Package version remains `0.2.0`; exports/runtime/schema are unchanged. Missing operational features include production credential issuance/rotation policy, real consumer/provider integration, provider batch partitioning/webhooks, key rotation, audited uncertain reconciliation/redrive, finite evidence-retention policy, production monitoring, sizing and HA. Current batch acceptance is atomic for one trusted principal/source. Payload cleanup is explicit and bounded; tombstones/receipts are retained. No production retention/SLA claim is made.

Stop after the Phase 1C mock proof for explicit acceptance. Live Meta/Pulse integration, HA and production deployment require separate authorization.

## Phase 1C mock provider gateway proof

`tests/provider-gateway.test.mjs` uses the actual XNode bootstrap and authenticated XNet host, two producer credentials and two outbound endpoint processes. Temporary stores and synthetic signed WhatsApp-shaped bytes exercise two receiving phones sharing one WABA. The gateway helper is private test code, excluded from published `dist` and exports. It has no Pulse, Agent or Channels dependency and exposes no provider HTTP endpoint or send operation.

Each invocation verifies exact raw-byte HMAC, validates exactly one supported text event, resolves an independently provisioned phone/WABA/account binding, and submits through the existing Wormholes AUTH/REQ path. Ambiguous identities, forged authority and all larger/unsupported batches fail before new persistence. Stable account-scoped keys preserve the original durable acceptance across retries/restarts; conflicting content is rejected. Durable gateway acceptance does not imply processing. Targets reuse the existing isolated `transport-worker.mjs` atomic effect/receipt transaction and send a processing ACK afterward.

Fifteen tests cover trust/isolation, boundary validation, least privilege, concurrent replay/conflicts, stored-policy changes, acceptance rollback/response loss, offline restart recovery, lost hints/receive/ACK responses, target and host crashes, target-transaction rollback, uncertainty, partial authentication failure, shutdown and log privacy. The existing 56 XNet regressions remain unchanged; the full suite contains 71 tests.

From `packages/`, after Core/Node/XNet builds, run on Node 22 with ephemeral loopback socket permission:

```sh
node --test modules/xnet/tests/provider-gateway.test.mjs
pnpm -C modules/xnet test
node --test xnode/tests/binding.test.mjs xnode/tests/lifecycle.test.mjs
```

The simulator accepts one event per invocation; it does not implement real Meta batches, webhook acknowledgment policy, outbound provider effects or distributed exactly-once execution. See [LLD sections 17–18](../../docs/xnet-technical-design-v0.1.md#18-phase-1c-mock-provider-gateway-implementation-record) for approved scope and verification evidence.
