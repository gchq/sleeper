REST API development
====================

Notes for developers changing the Sleeper REST API. For user-facing documentation, see the
[REST API overview](../rest-api/README.md).

## Where the spec lives

The API's shape is described by an OpenAPI 3.0 spec at
[docs/rest-api/openapi.yaml](../rest-api/openapi.yaml). The committed file is **auto-generated**
from the Java sources — do not hand-edit it. Its content comes from two places:

- The `Route` interface at [Route.java](../../java/rest-api/src/main/java/sleeper/restapi/Route.java) —
  each route declares its HTTP method, path, OpenAPI operation, and any component schemas it
  contributes.
- The generator at [GenerateOpenApiSpec.java](../../java/clients/src/main/java/sleeper/clients/deploy/documentation/GenerateOpenApiSpec.java)
  walks the route registry from `RestApiLambda.buildRoutes` and assembles the spec with a
  common header (info, servers, security scheme, shared `Error` schema).

## Adding a new endpoint

When you introduce a new endpoint:

1. Add the route implementation under [java/rest-api](../../java/rest-api/) and register it in
   `RestApiLambda#buildRoutes` so it is dispatched at runtime.
2. Implement `openApiMethod`, `openApiPath`, `openApiOperation`, and optionally `openApiSchemas`
   on the new route class so the generator picks it up. The existing `AddTableRoute` is the
   canonical example.
3. Add a prose companion page under [docs/rest-api/](../rest-api/) with a worked example, and
   link it from the endpoints table in the [REST API README](../rest-api/README.md).
4. Regenerate the spec (see below) and commit the updated `openapi.yaml` alongside the code
   change.

## Regenerating the spec

Run [scripts/dev/generateDocumentation.sh](../../scripts/dev/generateDocumentation.sh); the
final step invokes `GenerateOpenApiSpec` and overwrites
[docs/rest-api/openapi.yaml](../rest-api/openapi.yaml). You can also invoke the generator alone
from the `java/` directory:

```bash
mvn exec:java -q -pl clients \
  -Dexec.mainClass="sleeper.clients.deploy.documentation.GenerateOpenApiSpec" \
  -Dexec.args="$(pwd)/.."
```

## Verifying the committed spec

CI's diff of the working tree after running the generator is the source of truth — if the
regenerated `openapi.yaml` differs from the committed file, someone edited one without updating
the other. Regenerate, commit, and re-review.
