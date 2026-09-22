### Authentication

Set once for the whole configuration. Choose **Connection string (SAS)** or **Service principal (Entra ID)**, then fill in the fields for that method.

### Destination mapping (per row)

Each row sends one input table to one topic or queue:

- **Destination type & name** — the target topic or queue, which must already exist in the namespace.
- **Message body** — the whole row as JSON, or a single column's value.
- **Message properties** — optional mapping of input columns onto broker properties (message ID, session ID, subject, correlation ID, partition key, scheduled enqueue time, reply-to, application properties).
- **Delivery** — batch size, content type, and an optional per-message time-to-live.
