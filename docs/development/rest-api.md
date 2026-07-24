REST API development
====================

Notes for developers changing the Sleeper REST API. For user-facing documentation, see the
[REST API overview](../usage/rest-api/rest-api-overview.md).

## Adding a new endpoint

When you introduce a new endpoint:

1. Add the route implementation under [java/rest-api](../../java/rest-api/src/main/java/sleeper/restapi) and register it in
   `RestApiLambda#buildRoutes` so it is dispatched at runtime.
2. Add a prose companion page under [docs/rest-api/](../usage/rest-api/) with a worked example, and
   link it from the endpoints table in the [REST API overview](../usage/rest-api/rest-api-overview.md).
