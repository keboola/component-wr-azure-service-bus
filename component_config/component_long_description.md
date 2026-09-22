The Azure Service Bus writer sends each row of a Keboola Storage table as a message to an Azure Service Bus **topic** or **queue**.

Configuration is row-based: the connection to the namespace is set once at the configuration level, and each row maps one input table to one destination entity.

**Authentication** — connect with a shared access signature (SAS) connection string, or an Entra ID service principal (tenant ID, client ID, client secret, and namespace host name).

**Message body** — send the whole input row as a JSON object, or the value of a single column.

**Message properties** — optionally map input columns onto Service Bus broker properties such as message ID, session ID, subject, correlation ID, partition key, scheduled enqueue time, reply-to, and custom application properties.

**Delivery** — messages are sent in configurable batches, with an optional per-message time-to-live and content type.

Use the **Test Connection** action to verify the credentials and reach the target entity before running. The destination topic or queue must already exist in the namespace — the writer does not create it.
