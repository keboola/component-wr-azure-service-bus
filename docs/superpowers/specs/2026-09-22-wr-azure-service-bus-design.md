# keboola.wr-azure-service-bus — Design Spec

> Type: writer
> Component ID: keboola.wr-azure-service-bus
> Status: draft (awaiting maintainer approval)
> Date: 2026-09-22
> Precedent (config/UX model only, PHP+Node — not a code template): keboola.wr-azure-event-hub

## 1. Overview & target system

A Keboola **writer** that reads rows from a Keboola Storage input table and sends each row as a
message to an **Azure Service Bus** topic or queue. One input row becomes one Service Bus message;
the row's columns populate the message body and, optionally, first-class message attributes
(`message_id`, `session_id`, `subject`, `correlation_id`, `partition_key`, `application_properties`,
scheduled-enqueue time, etc.).

- **Target system:** Azure Service Bus (Standard/Premium namespaces), via the official Python SDK
  `azure-servicebus`. SDK docs: https://learn.microsoft.com/python/api/overview/azure/servicebus-readme
  · Quotas/limits: https://learn.microsoft.com/azure/service-bus-messaging/service-bus-quotas
- **Primary use case:** publish curated Storage data as events/commands onto a Service Bus entity so
  downstream Azure consumers (functions, logic apps, other services) can process them — with optional
  session-ordering and broker duplicate-detection driven from the data.

## 2. Keboola mapping

How Service Bus maps onto how Keboola runs a component.

- **Input tables → API writes.** The component reads one input table per config row and issues
  `ServiceBusSender.send_messages(...)` calls. It produces **no output tables** (see §6 and the
  grounding notes in §10) — the "output" is messages delivered to the broker.
- **Config rows (decided): one row = one destination mapping.** A message-bus writer commonly fans
  several Storage tables out to several entities, and each mapping is an independent unit that a user
  will want to enable, schedule, run, and retry on its own. Per the Keboola convention
  *"multiple independent objects → config rows"*, this component uses **config rows**: connection/auth
  lives at **config (root) level** (entered once); the destination entity, body mode, column/property
  mappings, and batch size live at **row level**. Each row's input-table mapping lives on the row (not
  the root), so multi-row runs don't cross-contaminate inputs.
  - *Override option for the maintainer:* if only ever one destination is expected, this can collapse
    to a single-config layout (the precedent `wr-azure-event-hub` is single-config). Recommendation is
    config rows; flagged for approval.
- **Execution model.** Rows execute **sequentially by default** (in `rowsSortOrder`); parallelism is
  **opt-in** (`parallelism`) and off unless the maintainer sets it. Do not assume rows run in parallel.
- **State.** This writer is **not incremental** and holds **no watermark** — every run sends the
  current input table in full. `state.json` is therefore **unused** (no read, no write). There is no
  per-row cursor. (Delivery idempotency is handled at the broker via `message_id` + entity
  duplicate-detection, not via Keboola state — see §6/G4.)
- **Secrets → `#`-prefixed keys.** `#connection_string` (SAS) and `#client_secret` (service
  principal) are `#`-prefixed so the platform encrypts them (`KBC::ProjectSecure` scope); the running
  container receives the decrypted plaintext.
- **Sync actions.** One sync action: `testConnection` (open a client/sender for the configured auth
  method, then close). Registered in the Developer Portal in Phase 6 so the platform can invoke it.
- **Output bucket / table naming.** N/A — no Storage output. Nothing is written under
  `/data/out/tables/`.
- **Token forwarding.** Not required — the component reads input via the Common Interface input
  mapping (files on disk), never the Storage API, so `forward_token`/`forward_token_details` stay off.

## 3. Authentication & connection

Per the maintainer's multi-auth directive, the writer ships a **pluggable credential abstraction** —
an `auth_type` discriminator selecting one concrete credential builder, structured so more methods can
slot in later. **v1 ships exactly two methods** (connection string + service principal). A managed-identity
path was scoped earlier but **removed by maintainer decision (2026-09-22)**: it carries no credential and
requires the container to run *as* an Azure identity, which Keboola does not provide today — so it cannot
work here. It may be re-added later if the platform team confirms Keboola can attach an Azure identity.

| # | Method | `auth_type` value | Fields | Headless? | v1 status |
|---|---|---|---|---|---|
| 1 | Connection string / SAS | `connection_string` | `#connection_string` | **Yes** — no interactive/admin step | **Default, fully exercisable** |
| 2 | Entra ID service principal | `service_principal` | `tenant_id`, `client_id`, `#client_secret`, `fully_qualified_namespace` | **Yes** (client-credentials, no interactive login) | **In scope**; live-testable only after a data-plane RBAC grant (see Provisioning) |
| ~~3~~ | ~~Managed Identity~~ | — | — | — | **Removed (2026-09-22)** — not runnable on Keboola; may return if the platform confirms identity attachment |

- **SDK surface.** Method 1 uses `ServiceBusClient.from_connection_string(conn_str=...)`. Method 2
  uses `ServiceBusClient(fully_qualified_namespace=..., credential=...)` where `credential` is an
  `azure-identity` `TokenCredential`: `ClientSecretCredential(tenant_id, client_id, client_secret)`.
  The connection is **AMQP 1.0 over TLS** (pyamqp transport) in all cases — no HTTP data plane (this
  drives the test approach in §7).
- **Send rights.** The SAS key or the service principal's role must grant **Send** on the target
  entity/namespace (the service principal needs the **"Azure Service Bus Data Sender"** RBAC role).
  Management/**Manage** rights are deliberately *not* required (no admin operations — see §4 H2).
- **Transport is fixed to pyamqp and never exposed.** The component must **not** set the deprecated
  `uamqp_transport` SDK parameter (deprecated in `azure-servicebus` 7.14.2, slated for removal in
  7.15.0 which has not shipped) and exposes **no transport-selection config field**. pyamqp's known
  multi-frame decode defect is on the **receive** path and does not affect a sender.

**Provisioning:**
- *Method 1 (connection string):* headless — the user pastes a Send-rights SAS connection string from
  the Azure portal (`Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...`).
  No admin/portal automation needed. The namespace is derived from the connection string's `Endpoint`.
- *Method 2 (service principal):* headless at runtime, but requires one-time Azure admin setup — an App
  Registration (client id + secret) **and** an **"Azure Service Bus Data Sender"** role assignment on
  the namespace/entity. The role assignment needs **Owner / User Access Administrator** on the scope
  (Contributor is insufficient — it cannot write role assignments). The user supplies `tenant_id`,
  `client_id`, `#client_secret`, and the `fully_qualified_namespace`.
- *Managed identity (removed 2026-09-22):* would have required an Azure-hosted identity, **not
  obtainable from Keboola's runtime**. Removed from v1 entirely; not just hidden. May be revisited if
  the platform team confirms Keboola can attach an Azure identity to the container.

**Blockers / access:**
- Method 2's data-plane role grant is an admin action (Owner/UAA). Until it is applied on the test
  namespace, the service-principal path is validated by **mocked-SDK** tests only, not a live send.
  Owner: maintainer.
- No blocker for v1's default path (connection string) — provisioning is clear and headless.

## 4. Capability inventory & scope

Baseline = the Phase-2 capability inventory (groups A–H). Every entry appears here with an explicit
verdict. Default posture is in-scope; exclusions carry a reason and are surfaced to the maintainer for
sign-off at spec approval (this spec is the sign-off artifact).

### A. Destination types
| Capability | Verdict | Rationale |
|---|---|---|
| A1. Topic (`get_topic_sender`) | **In scope** | Core target. |
| A2. Queue (`get_queue_sender`) | **In scope** | Same message/send model; only the sender factory + one field differ — cheap. |
| A3. Destination granularity | **In scope: one destination per config row** | Keboola config-rows convention; table-driven multi-routing within a single row is **Excluded (deferred)** — adds routing complexity with no v1 demand; revisit if asked. |

### B. Authentication modes
| Capability | Verdict | Rationale |
|---|---|---|
| B1. Connection string (SAS, Send) | **In scope (default)** | Headless, minimum viable, provisioned. |
| B2. Entra ID service principal (`ClientSecretCredential`) | **In scope** | Maintainer directive; headless client-credentials. Live-testable after the RBAC grant. |
| B2′. Managed Identity (`DefaultAzureCredential`) | **Excluded (removed by maintainer decision 2026-09-22; may add later if the platform confirms Keboola can attach an Azure identity)** | Not runnable on Keboola: the container does not run as an Azure identity, and MI carries no credential. Removed from the component entirely, not just hidden. |
| B3. `AzureSasCredential` / `AzureNamedKeyCredential` | **Excluded** | Niche; the abstraction leaves room to add it later. Sign-off: recorded here. |

### C. Send modes
| Capability | Verdict | Rationale |
|---|---|---|
| C1. Batched send (`create_message_batch` + fill loop) | **In scope (default)** | Throughput path. |
| C2. Single-message fallback (`send_messages(msg)`) | **In scope** | For a message too large for a batch but within the entity's single-message cap (relevant on Premium; on Standard it collapses into the oversized-row error path). |
| C3. Scheduled send | **In scope as a per-message property only** (E7) | Per-message `scheduled_enqueue_time_utc` is a cheap column mapping. `schedule_messages(...)` + cancellation (sequence-number bookkeeping) is **Excluded** — no re-run story in a stateless writer. Sign-off: recorded. |

### D. Row → body mapping
| Capability | Verdict | Rationale |
|---|---|---|
| D1. `row_as_json` (whole row → JSON object body) | **In scope (default)** | Precedent default. |
| D2. `column_value` (one named column is the body; JSON-decoded if valid JSON, else wrapped `{"data": "<raw>"}`) | **In scope** | Precedent parity. |
| D3. Body `content_type` (constant, default `application/json`, overridable) | **In scope** | Small config field. |

### E. Message property mappings (input column / constant → `ServiceBusMessage` attribute)
| Capability | Verdict | Rationale |
|---|---|---|
| E1. `message_id` (column) | **In scope** | Drives broker duplicate detection. |
| E2. `session_id` (column) | **In scope** | Required for session-enabled entities; enables ordered processing. |
| E3. `subject`/label (column) | **In scope** | Cheap, common. |
| E4. `correlation_id` (column) | **In scope** | Request/reply routing. |
| E5. `content_type` (column override) | **Excluded** | Covered as a constant by D3; a per-row column override is niche. Sign-off: recorded. Column-based override can be added later. |
| E6. `time_to_live` (constant seconds, row level) | **In scope (constant)** | Per-row constant TTL is the common case; a per-column TTL is **Excluded** (niche). |
| E7. `scheduled_enqueue_time_utc` (column, timestamp) | **In scope** | Realizes C3 as a property. |
| E8. `application_properties` (JSON-object column) | **In scope** | Mirrors the precedent's properties column. Invalid JSON → `UserException`. |
| E9. `partition_key` (column) | **In scope** | Partitioned entities / ordering. |
| E10. `reply_to` (column) | **In scope** | Cheap; full-scope-by-default. |
| E11. `reply_to_session_id` (column) | **In scope** | Cheap; full-scope-by-default. |
| E12. `to` | **Excluded** | Broker ignores it (auto-forward chaining only). Sign-off: recorded. |

### F. Throughput / batching controls
| Capability | Verdict | Rationale |
|---|---|---|
| F1. `batch_size` (messages per batch, default 1000) | **In scope** | Flush by count or by size, whichever first. |
| F2. `max_size_in_bytes` batch override | **Excluded (advanced)** | Let the SDK/tier default drive the byte cap; exposing it invites misconfiguration. Sign-off: recorded. |

### G. Error handling / robustness
| Capability | Verdict | Rationale |
|---|---|---|
| G1. Retry/backoff on transient `ServiceBusError`/AMQP | **In scope** | Use the SDK's built-in retry; tune `retry_total` rather than hand-rolling. |
| G2. Oversized-message handling | **In scope: fail the row (fail-fast)** | A row over the entity's single-message cap raises `UserException`. Silent split/skip is **Excluded** — do not fabricate delivery semantics. Sign-off: recorded. |
| G3. Clear `UserException` mapping | **In scope** | Bad/malformed connection string, auth failure (403), missing/typo'd entity (404 / entity-not-found), `session_id` required but missing, oversized message — each becomes an actionable exit-1 message with the access key redacted. |
| G4. At-least-once + no-double-send-on-close | **In scope** | After all batches are ack'd, a close/connection timeout is logged as a warning and the run still **succeeds** — reporting an error post-delivery would trigger a re-run and resend. |

### H. Sync actions
| Capability | Verdict | Rationale |
|---|---|---|
| H1. `testConnection` | **In scope** | Build the client for the configured auth method, open a sender to the entity, close. Connectivity + auth probe. |
| H2. Dynamic entity dropdown (list topics/queues via `ServiceBusAdministrationClient`) | **Excluded** | Needs **Manage** rights — exceeds the Send-only footprint a writer should require. Entity name stays free-text. Sign-off: recorded. |

**Mechanics for the in-scope surface (not pagination — a sender):**
- **No pagination.** The component streams the input CSV row-by-row; there is no API paging.
- **Rate/size limits.** The single hard limit is the broker message/batch size cap: **256 KB** on
  Basic/Standard (message *and* batch), and on Premium a single message up to **100 MB** over AMQP
  while a *batch* stays ~1 MB. The batching loop respects the SDK's `MessageSizeExceededError`
  (size-driven flush) as well as `batch_size` (count-driven flush); an individual row that overflows
  an empty batch is sent unbatched (C2), and one that exceeds the entity's single-message cap fails
  the row (G2). SDK retry handles transient throttling.
- **No bulk/async export.** Sending is synchronous (`send_messages` blocks until ack).

## 5. Configuration & schema

Layout: **config rows**. Connection/auth at config (root) level; destination + mapping at row level.
The actual `configSchema.json` / `configRowSchema.json` (with `options.dependencies` gating) is built
by `component-build-ui` in Phase 6 — described here, not written as JSON.

**Config-level (root) parameters — the auth block:**
- `auth_type` — enum, required: `connection_string` (default) · `service_principal`.
- `#connection_string` — secret, required when `auth_type=connection_string`.
- `tenant_id`, `client_id`, `#client_secret`, `fully_qualified_namespace` — required when
  `auth_type=service_principal` (`#client_secret` secret).

**Row-level parameters — one destination mapping:**
- `destination_type` — enum, required: `topic` · `queue`.
- `entity_name` — string, required (the topic or queue name; free-text, no dropdown — see H2).
- `mode` — enum, required: `row_as_json` (default) · `column_value`.
- `column` — string, required iff `mode=column_value`, forbidden otherwise (the body column).
- `content_type` — string, optional, default `application/json`.
- `batch_size` — integer ≥ 1, optional, default 1000.
- `time_to_live_seconds` — integer, optional (per-message TTL constant; capped to the entity TTL).
- `message_properties` — object, optional, holding the column-name mappings (each optional): 
  `message_id_column`, `session_id_column`, `subject_column`, `correlation_id_column`,
  `partition_key_column`, `scheduled_enqueue_time_column`, `application_properties_column`
  (a JSON-object column), `reply_to_column`, `reply_to_session_id_column`.
- Input table: the row's `storage.input.tables` supplies exactly one table; if the row's input mapping
  has exactly one table it is auto-selected, otherwise the run fails with a clear `UserException`
  (precedent's `tableId` auto-select behaviour, expressed via input mapping).

**Sync action:** `testConnection`.

### UI scope & config shape

Every field's exposure and persistence decided up front (Phase-7 fresh-config gate depends on this):

| Field | Level | Required | User-facing or internal | Default | In a NEW config's saved params? |
|---|---|---|---|---|---|
| `auth_type` | config | yes | user-facing (enum) | `connection_string` | yes — explicit choice, visible & self-explanatory |
| `#connection_string` | config | yes if `auth_type=connection_string` | user-facing (secret) | — | yes (user sets it); only when that auth branch is active |
| `tenant_id` / `client_id` / `#client_secret` / `fully_qualified_namespace` | config | yes if `auth_type=service_principal` | user-facing (`#client_secret` secret) | — | only when the service-principal branch is active (gated) |
| `destination_type` | row | yes | user-facing (enum) | none (explicit choice) | yes — no silent default |
| `entity_name` | row | yes | user-facing (text) | — | yes — user sets it |
| `mode` | row | yes | user-facing (enum) | `row_as_json` | yes — explicit, visible |
| `column` | row | yes iff `mode=column_value` | user-facing, gated on `mode` | — | only when `mode=column_value` |
| `content_type` | row | no | user-facing (text) | `application/json` | yes — visible & self-explanatory |
| `batch_size` | row | no | user-facing (integer) | 1000 | yes — visible & self-explanatory |
| `time_to_live_seconds` | row | no | user-facing (integer) | — (unset ⇒ entity default) | only if the user sets it |
| `message_properties.*_column` | row | no | user-facing, in a collapsible section | — | only the mappings the user fills |
| transport (pyamqp) | — | — | **internal — not in schema** | pyamqp | no |
| SDK `retry_total` | — | — | **internal — not in schema** | SDK default | no |

- **User-facing vs internal.** Transport and retry are derived/fixed and stay out of the schema (the
  Pydantic model may default them at runtime). Everything a user should set is exposed.
- **Persisted in a new config.** `auth_type`, `destination_type`, `mode` are **explicit choices** —
  they must be set by the user, so a fresh config records the user's selection, not a silent
  behaviour-changing default. Branch-specific fields (`column`, the service-principal credential
  fields) live behind `options.dependencies` and **must not** be serialized until their gate is
  active — otherwise a new config's diff shows unexplained values. `content_type` (`application/json`)
  and `batch_size` (1000) are visible, self-explanatory defaults and are acceptable to persist.

### UI presentation

| Field | Widget | Notes |
|---|---|---|
| `auth_type` | `enum` + `enum_titles` | stores the value (`service_principal`), shows a human label |
| `#connection_string` / `#client_secret` | password | secret inputs |
| `destination_type` | `enum` + `enum_titles` | topic/queue; stores value not label |
| `mode` | `enum` + `enum_titles` | `row_as_json`/`column_value`; stores value |
| `column`, `entity_name`, `*_column` | text | free-text column/entity names (no Manage-rights dropdown) |
| `batch_size`, `time_to_live_seconds` | integer | numeric inputs |
| `message_properties` | `type: object` section | grouped, collapsible — keeps the ~9 optional mappings out of the main flow |

Recurring review catches, pre-decided here:
- **No fused pickers / no extraction modes.** This is a writer: there is **no** `load_type`/`fetch_mode`
  (those are extractor concepts and are omitted). Do not add them.
- **Sectioning:** the auth branch fields and the `message_properties` mappings are grouped into named
  `type: object` sections (well past ~6 mixed fields once a branch is expanded).
- **Widget correctness:** every `enum` stores the machine value, never the label.

## 6. Code architecture

- **`src/configuration.py` (Pydantic).** `AuthConfig` as a discriminated union on `auth_type`
  (`ConnectionStringAuth` / `ServicePrincipalAuth` / `ManagedIdentityAuth`); `MessagePropertyMap`
  (the optional column-name fields); `RowConfiguration` (destination + mapping + `batch_size` +
  `content_type` + `time_to_live_seconds` + `message_properties`); top-level `Configuration`
  composing auth + row fields (the platform merges root+row into one `config.json`). Validation runs
  early; `ValidationError` is caught and re-raised as `UserException` (exit 1), as in the scaffold.
  Mutually-exclusive rules (`column` required iff `column_value`) are `model_validator`s.
- **`src/client.py` — credential factory + client.** `build_service_bus_client(auth: AuthConfig) ->
  ServiceBusClient` selects the concrete builder per `auth_type`
  (`from_connection_string` vs `ServiceBusClient(fully_qualified_namespace, credential=...)` with a
  `ClientSecretCredential`). Never sets `uamqp_transport`. This is the seam new auth methods slot into.
- **`src/sender.py` — send orchestration.** A `MessageSender` wrapping one `ServiceBusSender` (topic or
  queue): the fill-until-full batching loop (`create_message_batch` → `add_message` guarded by
  `MessageSizeExceededError`, flush on size overflow **or** at `batch_size` count, then re-add the
  overflowing message; a message overflowing an *empty* batch is sent unbatched, and if that also
  overflows the entity cap → `UserException`), plus close-safety (G4: swallow a post-delivery close
  timeout as a warning).
- **`src/message_builder.py` — row → `ServiceBusMessage`.** Builds the body per `mode`
  (`row_as_json` serializes the row dict; `column_value` takes the column, JSON-decoding if valid else
  wrapping `{"data": <raw>}`), sets `content_type`, applies each configured `message_properties` column
  onto the matching `ServiceBusMessage` attribute, parses the `application_properties` JSON column
  (bad JSON → `UserException`), and sets `time_to_live` / `scheduled_enqueue_time_utc` where given.
- **`src/component.py` — thin `run()` orchestrator.** load+validate config → resolve the single input
  table (auto-select / error) → `build_service_bus_client` → open the topic/queue sender → stream input
  rows, build messages, feed the batching loop → close safely. A `@sync_action` `test_connection`
  method builds the client and opens/closes a sender. `run()` delegates to private methods; no business
  logic inline. Keep the scaffold's top-level `UserException → exit 1` / `Exception → exit 2` guard.
- **Error handling.** `UserException` (exit 1) for user-fixable conditions in G3; unexpected failures
  bubble to exit 2. The SharedAccessKey / client secret are redacted from any surfaced message.
- **Logging.** stdout = INFO/WARNING, stderr = ERROR, so Keboola builds "Error details" from stderr.
- **Output column types / manifests.** **N/A** — the writer emits no output tables, so there are no
  manifests, no `schema`/`column_metadata`, and `dataTypeSupport` is not set. Any scratch file (none
  expected) would go to `/tmp`, never `data/out/tables/`.
- **Key dependencies.** `azure-servicebus>=7.14,<7.15` (pinned deliberately; pyamqp; avoids the
  `uamqp_transport` removal in 7.15.0) and `azure-identity` (for `ClientSecretCredential`, the
  service-principal path). `keboola-component` and `pydantic` from the scaffold.

## 7. Testing — enumerate the cases up front

**Key finding that shapes the whole test approach:** Azure Service Bus's data plane is **AMQP 1.0 over
TLS (pyamqp)**, *not* HTTP. `vcrpy` (the engine behind `keboola.datadirtest`'s VCR recording) intercepts
**HTTP** client libraries, so it **cannot record or replay the AMQP send path** — a pure sender makes no
HTTP calls, so classic cassettes would be empty. Therefore the local functional/unit suite exercises the
send path by **mocking the SDK boundary** (a fake `ServiceBusClient`/`ServiceBusSender` that captures the
`ServiceBusMessage`s and batches it was handed), and **real AMQP delivery is proven live in the Phase-7
cf-dev smoke test** against the provisioned namespace. This is a deliberate, verified deviation from the
"record HTTP with VCR" default, driven by the transport — not an omission. The `VCR_SANITIZERS`/log
sanitization discipline still applies to every captured artifact (logs, exit codes).

Cases (derived from §4 in-scope surface + §5 sync action + run modes):

| Case | Kind | Covers |
|---|---|---|
| `01_testConnection` | sync-action ok | auth + connectivity (fake client), `connection_string` |
| `02_testConnection_bad_conn_string` | sync-action fail (exit 1) | malformed connection string → redacted `UserException` |
| `03_run_queue_row_as_json` | run | queue target (`get_queue_sender`), `row_as_json` body, batched send |
| `04_run_topic_row_as_json` | run | topic target (`get_topic_sender`) |
| `05_run_column_value` | run | `column_value` body, JSON-decode + `{"data": …}` wrap fallback |
| `06_run_message_properties` | run | `message_id`/`session_id`/`subject`/`correlation_id`/`partition_key`/`application_properties`/`scheduled_enqueue_time` mappings applied to the message |
| `07_run_batch_flush_by_count` | run | `batch_size` boundary (count-driven flush) → ≥2 batches sent |
| `08_run_oversized_row` | run fail (exit 1) | a row exceeding the entity single-message cap → `UserException` |
| `09_run_missing_creds` | run fail (exit 1) | auth error path (no `#connection_string`) |
| `10_run_missing_entity_name` | run fail (exit 1) | missing `entity_name` |
| `11_run_empty_input` | run edge | zero data rows → success, no send calls |
| `12_run_service_principal_auth` | run | `auth_type=service_principal` → `ClientSecretCredential` selected (fake identity + client) |
| `13_run_bad_properties_json` | run fail (exit 1) | invalid JSON in the `application_properties` column → `UserException` |

- **Sanitizers.** Scrub from any captured log/fixture: the full `#connection_string` value and its
  `SharedAccessKey=<...>` fragment, the `#client_secret` value, and any `AZURE_CLIENT_SECRET`. Dummy
  credentials in `tests/setup/configs.json`; real values only in `secrets.json` (gitignored). The
  component itself redacts the access key in surfaced errors (G3), so it never reaches a log.
- **Input fixtures.** CSVs in `tests/setup/input_files/` (small row sets; one with a JSON column for
  `application_properties`, one with a session id column, one oversized row for `08`).
- **Coverage rules.** `component-test/references/vcr-configs-format.md` (wrapped format; every sync
  action has ≥1 passing + ≥1 failing test; every run mode covered; empty/edge cases included).

## 8. Deployment & validation (cf-dev)

Strategy only — **no concrete secrets, namespace names, subscription/tenant/SP identifiers are recorded
here** (they live in the gitignored local test-env note).

- **Branch image.** CI builds a dev image from the `initial-implementation` branch. Use the **newest**
  CI/dev image tag; a config's pinned `runtime.tag` can be stale, so bump it before running or the test
  exercises old code.
- **cf-dev config (via kbagent).** Create a config in the cf-dev project; override the image tag by
  setting **`runtime.tag` on the config** to the branch build (never by promoting the default tag or
  merging). Supply the Send-rights connection string as the encrypted `#connection_string`.
- **Entities exercised.** The maintainer-provisioned Standard-tier namespace (256 KB cap) offers a
  topic (with a read-back subscription), a plain queue, a **session-enabled** queue (validates
  `session_id`), and a **duplicate-detection-enabled** queue (validates `message_id` dedup) — covering
  topic/queue/session/dedup end-to-end.
- **Success looks like.** A real job with status `success`; the **resolved image tag matches the
  `initial-implementation` build** (confirm — a green job on a stale stable image is a false pass);
  messages verified delivered (read back via the topic's subscription / received from the queue). Send
  the same rows to the dedup queue twice with a fixed `message_id` and confirm a single delivery; send
  to the session queue and confirm `session_id` is preserved.
- **Fresh-config UI acceptance (runtime gate).** Create a *new* cf-dev config from scratch, save it,
  read the stored parameters back — no silent defaults, every `enum` stores the value not the label,
  the row-based config has `rows` ≥ 1.
- **Service principal path.** Live-testable only after the **"Azure Service Bus Data Sender"** RBAC
  grant is applied (needs Owner/UAA). Until then it is covered by mocked-SDK tests. **Managed identity**
  was removed from the component (2026-09-22) — not exercisable from a Keboola job, so there is no MI
  path to run.
- **Post-release.** A customer has offered real-traffic validation against their own Service Bus
  workload once a branch build exists — a strong extra confidence path after the smoke test.

## 9. Open risks & blockers

1. **AMQP ≠ HTTP → no classic VCR for the send path (highest impact on test design).** Local
   confidence comes from mocked-SDK functional tests; real-send confidence comes from the Phase-7 live
   run. Mitigation: build the SDK-boundary fake first; treat the cf-dev job as the send-path gate.
   Owner: implementation/test.
2. **Service-principal (B2) not yet live-testable.** The data-plane RBAC role grant requires
   Owner/User Access Administrator (the provisioning account has only Contributor). Owner: maintainer
   to run the grant; until then B2 is mock-validated.
3. **Managed Identity (B2′) removed from v1 (2026-09-22).** Not exercisable from a Keboola job (the
   container does not run as an Azure identity, and MI carries no credential), so it was removed from
   the component entirely — not just hidden. Re-add only if the platform team confirms Keboola can
   attach an Azure identity. Owner: platform team.
4. **Batch spanning multiple `session_id`s — [inferred, not verified].** Whether a single
   `ServiceBusMessageBatch` may carry messages with different `session_id`s to a session-enabled entity
   is not confirmed. Verify against the session-enabled queue in Phase 7; if unsupported, group by
   `session_id` (or send session-bound messages unbatched). Owner: implementation, verify at Phase 7.
5. **Oversized-row policy is fail-fast.** A row over the entity's single-message cap aborts that row's
   run with `UserException`. If a customer needs split/skip instead, that is a future opt-in, not v1.
6. **Duplicate detection is broker-side.** The writer only supplies `message_id`; dedup requires the
   target entity to have duplicate detection enabled. Validate against the dedup-enabled entity.

## 10. Grounding reconciliation (keboola-context)

Per-reference reconciliation of this spec against platform behaviour. Every behaviour-relevant
reference read in Phase 1 appears; each `corrected:` item is folded into the spec body above.

- **`architecture-conventions.md` → corrected:** the "two pickers — Load Type × Fetch Mode" convention
  is extractor-only and does **not** apply to a writer. Folded: §2/§5 state the component has no
  `load_type`/`fetch_mode` and no extraction modes; state is unused. All other conventions (config rows
  for multiple destinations, `#`-secrets, `testConnection` sync action, `UserException` exit-1/exit-2,
  client separated from `component.py` with a thin `run()`, one Pydantic model per group) are applied
  as stated — correct.
- **`config-rows.md` → corrected:** rows run **sequentially by default** (parallelism is opt-in), and
  the component always receives a single **merged** `config.json`; per-row `state.json` exists but this
  writer persists **no** state. Folded: §2 (execution model + state = unused), §5 (input mapping on the
  row), §7 (test fixtures = single merged `config.json`, no watermark state). Correct as folded.
- **`encryption.md` → correct:** `#connection_string` and `#client_secret` are `#`-prefixed →
  `KBC::ProjectSecure` at rest; the container receives plaintext. Stated in §2/§3/§5.
- **`exit-codes.md` → correct:** user-fixable → `UserException` (exit 1, message shown); unexpected →
  exit 2 (message hidden). Applied in §6/§4-G3, with access-key redaction on exit-1 messages.
- **`output-mapping.md` → corrected:** everything under `/data/out/tables/` is uploaded to Storage —
  but this writer emits **no** output tables, so nothing is written there; scratch files (none expected)
  go to `/tmp`. Folded: §2 (no output bucket), §6 (no manifests, `/tmp` scratch rule). Correct as folded.
- **`native-data-types.md` → corrected (N/A):** no output tables ⇒ no manifests ⇒ `dataTypeSupport` /
  `schema` vs `column_metadata` handling does not apply and is **not** set. Folded: §6 states manifests
  are N/A. Correct as folded.
- **`environment-variables.md` → correct:** `KBC_CONFIGROWID` is present per row and **absent (None)**
  on a full/non-row run — handle `None`; token variables (`KBC_TOKEN`/`KBC_URL`) are absent unless
  `forward_token` is enabled, which this writer does not need (it reads input via the Common Interface,
  not the Storage API). Folded: §2 (token forwarding off).
