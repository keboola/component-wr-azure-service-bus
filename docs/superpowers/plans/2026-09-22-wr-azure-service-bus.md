# Azure Service Bus Writer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each task names the **owner component skill** its subagent must load so it stays Keboola-aware.

**Goal:** Build the Keboola writer `keboola.wr-azure-service-bus` — reads one Storage input table per config row and sends each row as a message to an Azure Service Bus topic or queue, with a pluggable auth abstraction (connection string / service principal / managed-identity code path) and a rich per-message property mapping.

**Architecture:** Config-rows component. Root config = auth block; row = one destination mapping. Thin `run()` orchestrator in `component.py`; a credential factory (`client.py`), a row→message builder (`message_builder.py`), and a batching sender (`sender.py`). No output tables, no state. The Service Bus data plane is AMQP-over-TLS (not HTTP), so functional tests mock the SDK boundary and real delivery is proven in the Phase-7 cf-dev live run.

**Tech Stack:** Python 3.14, `azure-servicebus>=7.14,<7.15` (pyamqp), `azure-identity`, `keboola-component`, `pydantic` v2, `pytest`, `keboola.datadirtest`, `ruff`.

**Spec:** `docs/superpowers/specs/2026-09-22-wr-azure-service-bus-design.md` (read it alongside this plan — the plan argues from the spec).

## Global Constraints

Every task's requirements implicitly include these (values copied verbatim from the spec):

> **Update (2026-09-22): `managed_identity` was REMOVED from the component by maintainer decision.**
> v1 ships exactly two auth methods — `connection_string` and `service_principal`. The MI references
> below are historical; MI is no longer implemented (no `DefaultAzureCredential`, no `managed_identity`
> `auth_type`). It may return if the platform team confirms Keboola can attach an Azure identity.

- **Python 3.14** — `requires-python = "~=3.14.0"`; ruff `py314`. 3.14-only syntax is correct, not a bug.
- **SDK pin:** `azure-servicebus>=7.14,<7.15` on **pyamqp**. **NEVER set `uamqp_transport`.** Expose **no transport-selection config field**.
- **Auth deps:** `azure-identity` for `ClientSecretCredential` (service principal). (Historical: a `DefaultAzureCredential` managed-identity code path was planned but removed 2026-09-22.)
- **Secrets:** `#connection_string` and `#client_secret` are `#`-prefixed (platform-encrypted, `KBC::ProjectSecure`). Never a plain key. Redact the SAS `SharedAccessKey` / client secret from any surfaced error or log.
- **Config rows:** auth at config (root) level; destination + mapping at row level; each row's input mapping on the row. Rows run **sequentially by default** (parallelism opt-in, off unless set).
- **No output tables; `state.json` unused (not incremental).** Any scratch file → `/tmp`, never `data/out/tables/`. No manifests / `dataTypeSupport`.
- **Errors:** user-fixable → `UserException` (exit 1); unexpected → exit 2. Logging: stdout INFO/WARNING, stderr ERROR.
- **UI:** every `enum` stores the machine **value**, not the label; no silent behaviour-changing default persists in a fresh config; branch-gated fields don't serialize until active; row-based config needs `rows` ≥ 1.
- **Privacy:** no customer/company/person names, no ticket ids, no real secrets, and no concrete test namespace/subscription/tenant/SP identifiers in any committed file.

---

## Phase 4 — Implementation (owner skill per task: `component-develop`; schema task: `component-build-ui`)

### Task 1: Dependencies & scaffold cleanup

**Owner skill:** `component-develop` (consult `component-defaults` for pyproject/Dockerfile alignment).

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/configuration.py` (strip the `print_hello`/`api_token` example)
- Modify: `src/component.py` (strip the example `run()` body — replaced in Task 6)

**Interfaces:**
- Produces: `azure.servicebus` and `azure.identity` importable in the venv.

- [ ] **Step 1:** Add to `pyproject.toml` `[project].dependencies`: `"azure-servicebus>=7.14,<7.15"` and `"azure-identity>=1.19,<2"`. Keep `keboola-component`, `pydantic`. Remove `keboola-http-client` / `keboola-utils` if unused by the final design (this writer makes no HTTP calls).
- [ ] **Step 2:** Run `uv lock` then `uv sync --group dev`.
- [ ] **Step 3:** Verify import: `uv run python -c "import azure.servicebus, azure.identity; print(azure.servicebus.__version__)"` → prints a `7.14.x` version.
- [ ] **Step 4:** Remove the cookiecutter example fields from `configuration.py` and the example body from `component.py` (leave the `__main__` exit-code guard intact). `ruff check .` clean.
- [ ] **Step 5:** Commit.

```bash
git add pyproject.toml uv.lock src/configuration.py src/component.py
git commit -m "chore: add azure-servicebus + azure-identity deps, strip scaffold example"
```

---

### Task 2: Pydantic configuration model

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/configuration.py` (replace)
- Test: `tests/unit/test_configuration.py`

**Interfaces:**
- Produces:
  - `AuthType` (str enum): `CONNECTION_STRING="connection_string"`, `SERVICE_PRINCIPAL="service_principal"`, `MANAGED_IDENTITY="managed_identity"`.
  - `ConnectionStringAuth`, `ServicePrincipalAuth`, `ManagedIdentityAuth` (pydantic models); `AuthConfig` discriminated union on `auth_type`.
  - `DestinationType` (str enum): `TOPIC="topic"`, `QUEUE="queue"`.
  - `BodyMode` (str enum): `ROW_AS_JSON="row_as_json"`, `COLUMN_VALUE="column_value"`.
  - `MessagePropertyMap` (all `str | None`): `message_id_column, session_id_column, subject_column, correlation_id_column, partition_key_column, scheduled_enqueue_time_column, application_properties_column, reply_to_column, reply_to_session_id_column`.
  - `Configuration(BaseModel)` with fields: `auth_type: AuthType`, the auth fields (`connection_string` aliased `#connection_string`, `tenant_id`, `client_id`, `client_secret` aliased `#client_secret`, `fully_qualified_namespace`), `destination_type: DestinationType`, `entity_name: str`, `mode: BodyMode = ROW_AS_JSON`, `column: str | None`, `content_type: str = "application/json"`, `batch_size: int = 1000` (ge=1), `time_to_live_seconds: int | None`, `message_properties: MessagePropertyMap = MessagePropertyMap()`.
  - `Configuration(**params)` raises `UserException` on invalid input (catch `ValidationError`).

- [ ] **Step 1: Write failing tests** — `tests/unit/test_configuration.py`:

```python
import pytest
from keboola.component.exceptions import UserException
from configuration import Configuration, AuthType, DestinationType, BodyMode

BASE = {"auth_type": "connection_string", "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=s",
        "destination_type": "queue", "entity_name": "q1"}

def test_defaults():
    c = Configuration(**BASE)
    assert c.mode is BodyMode.ROW_AS_JSON
    assert c.content_type == "application/json"
    assert c.batch_size == 1000
    assert c.destination_type is DestinationType.QUEUE

def test_column_value_requires_column():
    with pytest.raises(UserException):
        Configuration(**{**BASE, "mode": "column_value"})  # no `column`

def test_column_forbidden_without_column_value():
    with pytest.raises(UserException):
        Configuration(**{**BASE, "column": "payload"})  # mode defaults row_as_json

def test_service_principal_requires_all_fields():
    with pytest.raises(UserException):
        Configuration(auth_type="service_principal", destination_type="queue", entity_name="q1",
                       tenant_id="t", client_id="c")  # missing #client_secret + namespace

def test_batch_size_min():
    with pytest.raises(UserException):
        Configuration(**{**BASE, "batch_size": 0})
```

- [ ] **Step 2:** Run `uv run pytest tests/unit/test_configuration.py -v` → FAIL (module/fields missing).
- [ ] **Step 3: Implement** `src/configuration.py`: the enums, the auth models, `Configuration` with `model_config = ConfigDict(populate_by_name=True)` and `Field(alias=...)` for the `#` keys; a `model_validator(mode="after")` enforcing `column` iff `mode==COLUMN_VALUE`, and the per-`auth_type` required-field rules; wrap `super().__init__` in try/except `ValidationError → UserException` (keep the scaffold pattern).
- [ ] **Step 4:** Run the tests → PASS. `ruff check .` clean.
- [ ] **Step 5:** Commit.

```bash
git add src/configuration.py tests/unit/test_configuration.py
git commit -m "feat: pydantic config model with pluggable auth + row mapping"
```

---

### Task 3: Credential factory & Service Bus client builder

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/client.py`
- Test: `tests/unit/test_client.py`

**Interfaces:**
- Consumes: `AuthConfig` fields from `Configuration` (Task 2).
- Produces: `build_service_bus_client(config: Configuration) -> ServiceBusClient`. Selects: `connection_string` → `ServiceBusClient.from_connection_string(conn_str=...)`; `service_principal` → `ServiceBusClient(fully_qualified_namespace=..., credential=ClientSecretCredential(tenant_id, client_id, client_secret))`; `managed_identity` → `ServiceBusClient(fully_qualified_namespace=..., credential=DefaultAzureCredential())`. **Never** passes `uamqp_transport`. Raises `UserException` on a malformed connection string, redacting the key.

- [ ] **Step 1: Write failing tests** — patch the SDK so no network is touched, assert the right constructor is called:

```python
from unittest import mock
from configuration import Configuration
import client as client_mod

BASE = {"auth_type": "connection_string", "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=SECRET",
        "destination_type": "queue", "entity_name": "q1"}

def test_connection_string_path():
    with mock.patch.object(client_mod, "ServiceBusClient") as sb:
        client_mod.build_service_bus_client(Configuration(**BASE))
        sb.from_connection_string.assert_called_once()
        assert "uamqp_transport" not in sb.from_connection_string.call_args.kwargs

def test_service_principal_path():
    cfg = Configuration(auth_type="service_principal", tenant_id="t", client_id="c",
                        **{"#client_secret": "sec"}, fully_qualified_namespace="ns.servicebus.windows.net",
                        destination_type="queue", entity_name="q1")
    with mock.patch.object(client_mod, "ServiceBusClient") as sb, \
         mock.patch.object(client_mod, "ClientSecretCredential") as cred:
        client_mod.build_service_bus_client(cfg)
        cred.assert_called_once_with("t", "c", "sec")
        sb.assert_called_once()
```

- [ ] **Step 2:** Run `uv run pytest tests/unit/test_client.py -v` → FAIL.
- [ ] **Step 3: Implement** `src/client.py`: `from azure.servicebus import ServiceBusClient`, `from azure.identity import ClientSecretCredential, DefaultAzureCredential`, the `build_service_bus_client` dispatch, and a `try/except` around connection-string parsing that raises `UserException("Invalid connection string …")` with the key redacted (regex-replace `SharedAccessKey=[^;]+`).
- [ ] **Step 4:** Run tests → PASS. `ruff check .` clean.
- [ ] **Step 5:** Commit.

```bash
git add src/client.py tests/unit/test_client.py
git commit -m "feat: credential factory + service bus client builder (pyamqp, no uamqp)"
```

---

### Task 4: Row → `ServiceBusMessage` builder

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/message_builder.py`
- Test: `tests/unit/test_message_builder.py`

**Interfaces:**
- Consumes: `Configuration` (Task 2); a row `dict[str, str]` (CSV `DictReader` row).
- Produces: `build_message(row: dict, config: Configuration) -> ServiceBusMessage`. Body per `mode`; sets `content_type`; applies each configured `message_properties.*_column` onto the matching `ServiceBusMessage` attribute; parses the `application_properties` JSON column (bad JSON → `UserException`); sets `time_to_live` (from `time_to_live_seconds`) and `scheduled_enqueue_time_utc` (parsed ISO-8601 from the column). `MessageMappingError(UserException)` for row-level mapping failures.

- [ ] **Step 1: Write failing tests:**

```python
import json, pytest
from configuration import Configuration
from message_builder import build_message
from keboola.component.exceptions import UserException

BASE = {"auth_type": "connection_string", "#connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=s",
        "destination_type": "queue", "entity_name": "q1"}

def test_row_as_json_body():
    msg = build_message({"a": "1", "b": "x"}, Configuration(**BASE))
    assert json.loads(str(msg)) == {"a": "1", "b": "x"}
    assert msg.content_type == "application/json"

def test_column_value_json_decoded():
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message({"payload": '{"k": 1}'}, cfg)
    assert json.loads(str(msg)) == {"k": 1}

def test_column_value_wraps_non_json():
    cfg = Configuration(**{**BASE, "mode": "column_value", "column": "payload"})
    msg = build_message({"payload": "raw text"}, cfg)
    assert json.loads(str(msg)) == {"data": "raw text"}

def test_property_mappings():
    cfg = Configuration(**{**BASE, "message_properties": {"session_id_column": "sid", "message_id_column": "mid"}})
    msg = build_message({"a": "1", "sid": "s1", "mid": "m1"}, cfg)
    assert msg.session_id == "s1" and msg.message_id == "m1"

def test_bad_application_properties_json_raises():
    cfg = Configuration(**{**BASE, "message_properties": {"application_properties_column": "props"}})
    with pytest.raises(UserException):
        build_message({"a": "1", "props": "{not json"}, cfg)
```

- [ ] **Step 2:** Run `uv run pytest tests/unit/test_message_builder.py -v` → FAIL.
- [ ] **Step 3: Implement** `src/message_builder.py` per the interface. For `row_as_json`, `body = json.dumps(row)`. For `column_value`, try `json.loads(value)`, else `json.dumps({"data": value})`. Map the columns onto attributes; skip a mapping whose column is unset or absent from the row. Parse `application_properties` with `json.loads` (must be a JSON object) → `msg.application_properties`. Parse the scheduled column as ISO-8601 → aware UTC `datetime`; TTL as `timedelta(seconds=...)`.
- [ ] **Step 4:** Run tests → PASS. `ruff check .` clean.
- [ ] **Step 5:** Commit.

```bash
git add src/message_builder.py tests/unit/test_message_builder.py
git commit -m "feat: row-to-ServiceBusMessage builder (both modes + property mappings)"
```

---

### Task 5: Batching sender

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/sender.py`
- Test: `tests/unit/test_sender.py`

**Interfaces:**
- Consumes: a `ServiceBusSender`-like object (real or fake), `batch_size: int`.
- Produces: `MessageSender(sender, batch_size)` with `.send(messages: Iterable[ServiceBusMessage]) -> int` (returns count sent) and `.close()`. Batching: `create_message_batch()` → `add_message` guarded by `MessageSizeExceededError`; flush on size overflow **or** at `batch_size` count; re-add the overflowing message to a fresh batch; a message overflowing an **empty** batch → `send_messages(msg)` individually; if that also raises `MessageSizeExceededError` → `UserException` (oversized for the entity). `.close()` swallows a post-send close error as a WARNING (G4 no-double-send).

- [ ] **Step 1: Write failing tests** using a fake sender/batch that raises `MessageSizeExceededError` on demand:

```python
from azure.servicebus.exceptions import MessageSizeExceededError
from keboola.component.exceptions import UserException
from sender import MessageSender
import pytest

class FakeBatch:
    def __init__(self, cap): self.cap, self.msgs = cap, []
    def add_message(self, m):
        if len(self.msgs) >= self.cap: raise MessageSizeExceededError(message="full")
        self.msgs.append(m)

class FakeSender:
    def __init__(self, cap=2): self.cap, self.sent_batches, self.sent_singles = cap, [], []
    def create_message_batch(self): return FakeBatch(self.cap)
    def send_messages(self, x):
        if isinstance(x, FakeBatch): self.sent_batches.append(list(x.msgs))
        else: self.sent_singles.append(x)
    def close(self): pass

def test_flush_by_size_overflow():
    fs = FakeSender(cap=2)
    n = MessageSender(fs, batch_size=1000).send(["a", "b", "c"])  # cap 2 → 2 batches
    assert n == 3 and len(fs.sent_batches) == 2

def test_flush_by_count():
    fs = FakeSender(cap=1000)
    MessageSender(fs, batch_size=2).send(["a", "b", "c", "d"])
    assert [len(b) for b in fs.sent_batches] == [2, 2]
```

- [ ] **Step 2:** Run `uv run pytest tests/unit/test_sender.py -v` → FAIL.
- [ ] **Step 3: Implement** `src/sender.py` per the interface (import `MessageSizeExceededError` from `azure.servicebus.exceptions`). Track a running count; flush at `batch_size`. On the empty-batch overflow, `send_messages(msg)`; wrap its `MessageSizeExceededError` as `UserException("Message … exceeds the entity size limit …")`.
- [ ] **Step 4:** Run tests → PASS. `ruff check .` clean.
- [ ] **Step 5:** Commit.

```bash
git add src/sender.py tests/unit/test_sender.py
git commit -m "feat: batching sender with size/count flush + oversized-row UserException"
```

---

### Task 6: Component orchestrator + testConnection sync action

**Owner skill:** `component-develop`.

**Files:**
- Modify: `src/component.py`
- Test: `tests/unit/test_component.py`

**Interfaces:**
- Consumes: `build_service_bus_client` (T3), `build_message` (T4), `MessageSender` (T5), `Configuration` (T2).
- Produces: `Component.run()` (thin orchestrator) and `@sync_action("testConnection") test_connection(self) -> dict`. `run()`: validate config → resolve the single input table (exactly one, else `UserException`) → build client → `get_topic_sender`/`get_queue_sender` by `destination_type` → stream rows (`csv.DictReader`) through `build_message` → `MessageSender.send(...)` → close safely. Empty input → success, no send.

- [ ] **Step 1: Write failing tests** patching `build_service_bus_client` so no network is touched; drive `run()` off a temp data dir with one input CSV; assert the fake sender received N messages, and that zero-row input sends nothing; assert missing input table raises `UserException`.
- [ ] **Step 2:** Run `uv run pytest tests/unit/test_component.py -v` → FAIL.
- [ ] **Step 3: Implement** `run()` and `test_connection` in `component.py` with logic in private methods (`_resolve_input_table`, `_open_sender`, `_stream_and_send`). Register the sync action via the `keboola-component` `@sync_action` decorator. Keep the `__main__` exit-code guard. Ensure stdout/stderr log split (INFO/WARN → stdout, ERROR → stderr) is configured.
- [ ] **Step 4:** Run tests → PASS. `ruff check .` clean.
- [ ] **Step 5:** Commit.

```bash
git add src/component.py tests/unit/test_component.py
git commit -m "feat: thin run() orchestrator + testConnection sync action"
```

---

### Task 7: Config UI — configSchema + configRowSchema + sample-config

**Owner skill:** `component-build-ui` (this is schema/UI work — dispatch to the UI specialist).

**Files:**
- Modify: `component_config/configSchema.json` (root = auth block)
- Modify: `component_config/configRowSchema.json` (row = destination + mapping)
- Modify: `component_config/sample-config/` (a valid example, dummy secrets)

**Interfaces:**
- Consumes: the field list & UI-presentation table from spec §5.
- Produces: schemas whose keys exactly match the Pydantic model (Task 2), enums storing **values** not labels, `options.dependencies` gating.

- [ ] **Step 1:** Build `configSchema.json` (root): `auth_type` enum (`enum` + `enum_titles`, default `connection_string`); `#connection_string` (password, shown when `auth_type=connection_string`); `tenant_id`/`client_id`/`#client_secret`/`fully_qualified_namespace` (shown when `auth_type=service_principal`); `fully_qualified_namespace` (shown when `auth_type=managed_identity`) — all via `options.dependencies` so branch fields don't serialize until active.
- [ ] **Step 2:** Build `configRowSchema.json` (row): `destination_type` enum (no default — explicit choice), `entity_name` text, `mode` enum (default `row_as_json`), `column` (shown when `mode=column_value`), `content_type` text (default `application/json`), `batch_size` integer (default 1000, min 1), `time_to_live_seconds` integer (optional), and a `message_properties` `type: object` collapsible section with the nine optional `*_column` text fields.
- [ ] **Step 3:** Validate the schemas (schema-tester / the UI skill's tooling); confirm every `enum` stores value not label and no non-gated default beyond `content_type`/`batch_size`.
- [ ] **Step 4:** Update `component_config/sample-config/` to a valid row-based example with **dummy** credentials.
- [ ] **Step 5:** Commit.

```bash
git add component_config/
git commit -m "feat: config + row schemas (auth branches, destination mapping, property section)"
```

---

## Phase 5 — Tests & cassettes (owner skill: `component-test` / `generate-vcr-tests`)

### Task 8: Functional (datadir) suite + SDK-mock harness + sanitizers

**Owner skill:** `component-test` (note the AMQP-not-HTTP finding in spec §7 — this suite mocks the SDK boundary; it does **not** record AMQP with vcrpy).

**Files:**
- Create: `tests/setup/configs.json`
- Create: `tests/setup/input_files/*.csv`
- Create: `tests/functional/test_functional.py`
- Create/Modify: `tests/conftest.py` (autouse fixture that patches `client.ServiceBusClient` with a message-capturing fake for run/sync-action cases)
- Create: `secrets.json` skeleton note (real values gitignored)

**Interfaces:**
- Consumes: the component entrypoint; the case list from spec §7.
- Produces: a green functional suite covering the 13 cases; sanitizers scrubbing the connection string / client secret from all captured logs.

- [ ] **Step 1:** Author `tests/setup/configs.json` (wrapped format) for the 13 cases in spec §7 — dummy credentials only. Include a `testConnection` sync-action case + its failure case, run cases for queue/topic/`row_as_json`/`column_value`/property-mappings/batch-flush, and the exit-1 failures (oversized row, missing creds, missing entity, bad properties JSON) and the empty-input edge.
- [ ] **Step 2:** Add input CSVs under `tests/setup/input_files/` (a small rows table; one with a JSON `props` column; one with a `sid` session column; one with an oversized cell for the oversized case).
- [ ] **Step 3:** Add `tests/conftest.py` with an autouse fixture that patches `client.ServiceBusClient` to a fake capturing sent messages/batches, so run/sync-action cases execute offline and deterministically; failure cases that fail before any send need no fake.
- [ ] **Step 4:** Wire log sanitization: scrub `SharedAccessKey=[^;]+`, the full `#connection_string` and `#client_secret` values, and `AZURE_CLIENT_SECRET` from captured logs (`VCR_SANITIZERS`-style hooks in `component.py` / the test harness). Confirm the component redacts the access key in error messages.
- [ ] **Step 5:** Run `uv run pytest -v`. Paste the `N passed` line. `ruff check .` clean.
- [ ] **Step 6:** Grep every captured fixture/log for secret patterns → clean. Commit.

```bash
git add tests/ secrets.json.dist
git commit -m "test: functional suite with SDK-mock harness + secret sanitizers"
```

---

## Self-Review

**1. Spec coverage.** Every in-scope capability from spec §4 maps to a task: A1/A2 destination senders (T6), B1/B2/B2′ auth (T2+T3), C1/C2 send modes + oversized (T5), D1/D2/D3 body modes (T4), E1–E11 property mappings (T4), F1 batch_size (T2+T5), G1–G4 error handling/close-safety (T3/T5/T6), H1 testConnection (T6). Excluded items (A3 table-routing, B3, C3 schedule_messages, E5 col content-type, E6 col TTL, E12 `to`, F2, G2 split/skip, H2 dropdown) carry recorded reasons in the spec and are intentionally not tasks. UI/config shape → T7. Testing (§7) → T8. cf-dev (§8) is Phase 7, not this plan.

**2. Placeholder scan.** No `TBD`/`TODO`; each code step carries real code or a concrete instruction; failure-handling steps name the exact exception → exit mapping.

**3. Type consistency.** Names are stable across tasks: `Configuration`, `AuthType`/`DestinationType`/`BodyMode`, `MessagePropertyMap`, `build_service_bus_client`, `build_message`, `MessageSender.send/.close`. Schema keys (T7) match the Pydantic field names/aliases (T2).

## Execution Handoff

Execute via **superpowers:subagent-driven-development** — one fresh subagent per task, loading the task's named owner skill (`component-develop`, `component-build-ui`, or `component-test`), with a two-stage review between tasks. Phase 4 = Tasks 1–7; Phase 5 = Task 8. Tick the lifecycle tracker's Phase 4/5 boxes only on the independent verifier's ✅, not when the last task's checkbox is filled.
