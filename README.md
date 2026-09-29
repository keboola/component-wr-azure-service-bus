Azure Service Bus Writer
========================

Sends each row of a Keboola Storage table as a message to an Azure Service Bus
**topic** or **queue**.

**Table of Contents:**

[TOC]

Functionality
=============

The writer reads one input table per configuration row and publishes every row
as a message to the configured Service Bus entity. Messages are sent in batches
over the AMQP protocol. The target topic or queue must already exist in the
namespace — the writer does not create it.

Configuration is **row-based**: the connection to the namespace is set once at the
configuration level, and each row maps one input table to one destination entity
with its own body, property, and delivery settings.

Prerequisites
=============

- An Azure Service Bus namespace with the target **topic** or **queue** already
  created.
- Credentials that grant **Send** rights on the entity, provided through one of
  the supported authentication methods below.

Authentication
==============

Set once for the whole configuration. Choose one method:

- **Connection string (SAS)** — a shared access signature connection string for
  the namespace or entity, with Send rights.
- **Service principal (Entra ID)** — a Microsoft Entra ID application identity:
  tenant ID, client ID, client secret, and the fully qualified namespace host
  name (e.g. `my-namespace.servicebus.windows.net`).

Configuration
=============

Destination mapping (per row)
-----------------------------

Each row sends one input table to one topic or queue:

- **Destination type & name** — `topic` or `queue`, plus the entity name. The
  entity must already exist in the namespace.
- **Message body** — one of:
    - `row_as_json` — the whole input row serialized as a UTF-8 JSON object
      (non-ASCII text is kept as-is, not escaped).
    - `column_value` — the chosen column's value, sent as the message body
      exactly as-is (no JSON parsing or wrapping). Set **Content Type** to match
      the payload (e.g. `text/plain` for plain text, `application/json` if the
      column already holds a JSON string).
- **Message properties** — optionally map input columns onto Service Bus broker
  properties: message ID, session ID, subject, correlation ID, partition key,
  reply-to, reply-to session ID, scheduled enqueue time (ISO-8601), and custom
  application properties (a JSON object whose values must be scalars — string,
  number, boolean, or null). A configured column that is missing from the input
  table header fails the run with a clear error. A **blank cell** in a mapped
  column simply skips that property for that row, so a table where only some rows
  carry a property is fine.
    - Mapping a stable **Message ID** column lets Azure **duplicate detection**
      (if enabled on the entity) discard repeats — so re-running a failed job
      does not deliver the already-sent messages twice. Without it the broker
      assigns a fresh random ID to every message and cannot deduplicate.
- **Content type** — set per message. In `row_as_json` mode the body is always a
  JSON object, so the content type is always `application/json`. In `column_value`
  mode the content type is configurable (default `application/json`), since the
  column's raw value may be any MIME type.
- **Delivery** — batch size. The writer does not set a per-message time-to-live;
  message expiry follows the destination queue's or topic's own configured default
  TTL (`default_message_time_to_live`).

Test Connection
---------------

The **Test Connection** sync action verifies the selected authentication method
and reaches the target entity by opening the AMQP link, without sending any
message. Use it to validate credentials and entity access before running.

Output
======

This is a writer: it produces no Storage output tables. Its output is the
messages delivered to the Service Bus topic or queue.

Performance
===========

Measured on the component's test setup:

| Setting | Value |
|---|---|
| Keboola stack | GCP `us-east4` |
| Service Bus | Standard tier namespace in West Europe; queue without partitioning, sessions, or duplicate detection |
| Input | 1,000,000 rows, 5 short columns (~105 B JSON body per message) |
| Configuration | Whole row as JSON, batch size 5000, no message properties |
| Result | **1,000,000 messages in 724 s** of job time (incl. start-up and input read), about **1,380 messages/s** |

Batches are capped by the tier's 256 KB message limit, so with rows this small a
batch fills at about 1,400 messages; a batch size above that does not speed up
small rows. Larger rows fit fewer messages per batch. A Premium namespace (1 MB
batches) or one closer to the Keboola stack should send faster.

Development
===========

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder
with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the
following commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone  component-wr-azure-service-bus
cd component-wr-azure-service-bus
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
