### Authentication

Set once for the whole configuration. Choose **Connection string (SAS)** or **Service principal (Entra ID)**, then fill in the fields for that method.

### Destination mapping (per row)

Each row sends one input table to one topic or queue:

- **Destination type & name** — the target topic or queue, which must already exist in the namespace.
- **Message body** — the whole row as a UTF-8 JSON object, or a single column's raw value. Content type is always `application/json` for whole-row mode and settable for single-column mode.
- **Message properties** — optional mapping of input columns onto broker properties (message ID, session ID, subject, correlation ID, partition key, scheduled enqueue time, reply-to, application properties). A blank cell skips that property for the row.
- **Delivery** — batch size. Message expiry follows the destination entity's own default TTL; the writer sets no per-message TTL.
