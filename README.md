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
    - `row_as_json` — the whole input row serialized as a JSON object.
    - `column_value` — the value of a single named column, passed through when it
      is valid JSON and otherwise wrapped as `{"data": <raw value>}`.
- **Message properties** — optionally map input columns onto Service Bus broker
  properties: message ID, session ID, subject, correlation ID, partition key,
  reply-to, reply-to session ID, scheduled enqueue time (ISO-8601), and custom
  application properties (a JSON object). A configured column that is missing from
  the input table header fails the run with a clear error.
- **Delivery** — batch size, content type (default `application/json`), and an
  optional per-message time-to-live in seconds.

Test Connection
---------------

The **Test Connection** sync action verifies the selected authentication method
and reaches the target entity by opening the AMQP link, without sending any
message. Use it to validate credentials and entity access before running.

Output
======

This is a writer: it produces no Storage output tables. Its output is the
messages delivered to the Service Bus topic or queue.

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
