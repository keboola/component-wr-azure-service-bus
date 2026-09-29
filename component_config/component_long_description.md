The Azure Service Bus writer sends each row of a Keboola Storage table as a message to an Azure Service Bus **topic** or **queue**.

Configuration is row-based: the connection to the namespace is set once at the configuration level, and each row maps one input table to one destination entity.

**Authentication** — connect with a shared access signature (SAS) connection string, or an Entra ID service principal (tenant ID, client ID, client secret, and namespace host name).

**Message body** — send the whole input row as a UTF-8 JSON object, or the raw value of a single column. Content type is always `application/json` for whole-row mode and settable for single-column mode.

**Message properties** — optionally map input columns onto Service Bus broker properties such as message ID, session ID, subject, correlation ID, partition key, scheduled enqueue time, reply-to, and custom application properties.

**Delivery** — messages are sent in configurable batches. Message expiry follows the destination entity's own default time-to-live; the writer sets no per-message TTL.

Use the **Test Connection** action to verify the credentials and reach the target entity before running. The destination topic or queue must already exist in the namespace — the writer does not create it.
