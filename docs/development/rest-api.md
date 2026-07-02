REST API development
====================

Notes for developers changing the Sleeper REST API. For user-facing documentation, see the
[REST API overview](../rest-api/README.md).

## Where the spec lives

The source of truth for the API's shape is [docs/rest-api/openapi.yaml](../rest-api/openapi.yaml).
It is hand-authored and committed to the repo. Update it whenever an endpoint's method, path,
request body, response body, or error contract changes.

## Adding a new endpoint

When you introduce a new endpoint:

1. Register the route in `RestApiLambda#registerRoutes` (in the `java/rest-api` module) so it
   maps to a `Route` implementation.
2. Add the path and operation to [openapi.yaml](../rest-api/openapi.yaml), together with any new
   request or response schemas under `components.schemas`.
3. Add a prose companion page under [docs/rest-api/](../rest-api/) with a worked example, and
   link it from the endpoints table in the [REST API README](../rest-api/README.md).

Prefer to keep the OpenAPI schemas as the primary description of request/response shapes; the
prose page should focus on how a user invokes the endpoint end-to-end.

## Regenerating the spec from a deployed API

The stack is deployed as an API Gateway v2 HTTP API, so use the `apigatewayv2` export command
(not `apigateway`, which is for v1 REST APIs):

The `--api-id` argument is the subdomain of the invoke URL, not the full URL. If
`sleeper.rest.api.url` is `https://abcd1234.execute-api.eu-west-2.amazonaws.com`, then the id
is `abcd1234`.

```bash
API_ID=<the apiId from sleeper.rest.api.url>

aws apigatewayv2 export-api \
    --api-id "$API_ID"      \
    --specification OAS30   \
    --output-type YAML      \
    openapi.exported.yaml
```

See the [AWS docs on exporting an HTTP API](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-export.html)
for the full command reference.

## Verifying the committed spec

Diff the exported file against [openapi.yaml](../rest-api/openapi.yaml). Focus on paths,
operations, and schemas — the hand-written spec deliberately omits AWS-specific `x-amazon-apigateway-*`
extensions and the API Gateway assigned `servers` URL, so those will differ.

If you find drift, treat the deployed API as evidence and update either the code or the
committed spec to bring them back into sync.
