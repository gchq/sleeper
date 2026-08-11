Add table
=========

`POST /sleeper/tables`

Creates a new table in a deployed Sleeper instance. Equivalent to running the
[`addTable.sh` script](../tables.md#using-scripts), but callable over HTTPS from any AWS-authenticated
client.

## Prerequisites

- The Sleeper instance is deployed with `RestApiStack` in `sleeper.optional.stacks`.
- The caller's IAM identity has `execute-api:Invoke` on the API.
- You know the invoke URL (from CDK output `RestApiUrl` or instance property
  `sleeper.rest.api.url`).

## Request

The body is JSON with three fields:

| Field         | Type                          | Required | Description                                                                                                                                                                                              |
| ------------- | ----------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `properties`  | object of string → string     | Yes      | Sleeper [table properties](../property-master.md). Must include `sleeper.table.name`; other properties fall back to instance defaults.                                                             |
| `schema`      | object                        | Yes      | The [Sleeper schema](../schema.md) for the new table.                                                                                |
| `splitPoints` | array of strings              | No       | Pre-split points for the table's partition tree, as string values of the row key column. Only supported for tables with a single row key field; sending them for a multi-row-key table returns `400`.   |

## Example

Sign the request with SigV4. The example below uses `curl`'s built-in `--aws-sigv4` flag
(available in curl 7.75 and later, which is what the [Sleeper Builder](../../deployment/docker-tools.md) container ships with).

```bash
INSTANCE_ID=my-instance
ACCOUNT_ID=my-account-id
AWS_REGION=my-region
REST_API_URL=$(aws s3 cp "s3://sleeper-${INSTANCE_ID}-config-${ACCOUNT_ID}/instance.properties" - | grep '^sleeper.rest.api.url=' | cut -d= -f2-)

# Load AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and (if using an instance role
# or assumed role) AWS_SESSION_TOKEN into the environment.
eval "$(aws configure export-credentials --format env)"

curl --aws-sigv4 "aws:amz:${AWS_REGION}:execute-api" \
     --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" \
     -H "x-amz-security-token: ${AWS_SESSION_TOKEN}"        \
     -H 'Content-Type: application/json'                    \
     -X POST -d @- "${REST_API_URL}/sleeper/tables" <<'JSON'
{
  "properties": {
    "sleeper.table.name": "my-table"
  },
  "schema": {
    "rowKeyFields": [
      { "name": "key", "type": "StringType" }
    ],
    "sortKeyFields": [
      { "name": "timestamp", "type": "LongType" }
    ],
    "valueFields": [
      { "name": "value", "type": "StringType" }
    ]
  },
  "splitPoints": ["m"]
}
JSON
```

The instance property lookup above retrieves the URL from the instance config bucket; if you already have the URL to
hand, skip that step. The `x-amz-security-token` header is only required for temporary credentials
(EC2 instance roles, `aws sts assume-role`, SSO); omit it if you are signing with a long-lived IAM
user access key.

### Using `awscurl` instead

If you already have [`awscurl`](https://github.com/okigan/awscurl) installed, the same request is
shorter — `awscurl` picks up all three credential values automatically:

```bash
awscurl --service execute-api --region "$AWS_REGION" \
        -X POST -H 'Content-Type: application/json'  \
        -d @request.json "${REST_API_URL}/sleeper/tables"
```

## Responses

### `201 Created`

The table was created. The response echoes the assigned id and name:

```json
{
  "tableId": "01HXYZABCDEFGHJKMNPQRSTVWX",
  "tableName": "my-table"
}
```

### `400 invalid_request`

The request was rejected before any state was changed. Triggered by:

- A body that is not valid JSON, or is empty.
- Missing `properties` or `schema`.
- Split points that cannot be parsed against the schema's row key type, or supplied for a table
  with more than one row key field.
- Table property values that fail Sleeper's own validation.

```json
{
  "error": "invalid_request",
  "message": "Request must include 'schema'"
}
```

### `409 table_already_exists`

A table with the same name already exists in the instance. Rename the table (or delete the
existing one with [`deleteTable.sh`](../tables.md#renamedelete-a-table)) and retry.

```json
{
  "error": "table_already_exists",
  "message": "Table with name 'my-table' already exists"
}
```

### `500 internal_error`

The request failed unexpectedly inside the lambda. Consult the REST API lambda log group
(`/aws/lambda/sleeper-<instance-id>-rest-api-handler`) for the stack trace.

## See also

- [Tables documentation](../tables.md) - full context on creating and managing tables.
- [`addTable.sh` script](../tables.md#using-scripts) - the local-shell alternative.
