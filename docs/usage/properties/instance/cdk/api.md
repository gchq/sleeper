## Instance Properties - API - CDK Defined

The following instance properties relate to the Sleeper REST API, including how clients authenticate and which clients are authorised.

| Property Name                      | Description                                                                                                                                                               |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sleeper.rest.api.url               | The URL for the Sleeper REST API.                                                                                                                                         |
| sleeper.rest.api.token.url         | The URL of the API endpoint that authenticates callers by a token from an OIDC issuer, which also serves the UI.                                                          |
| sleeper.rest.api.iam.url           | The URL of the API endpoint that authenticates callers by their IAM role.                                                                                                 |
| sleeper.rest.api.cognito.issuer    | The issuer identifier of the Cognito user pool created for the API, matching the iss claim in the tokens it signs. Only set if a user pool was created.                   |
| sleeper.rest.api.cognito.client.id | The ID of the application the Cognito user pool signs users in to. Tokens are only accepted if they were meant for this application. Only set if a user pool was created. |
| sleeper.rest.api.cognito.login.url | The URL of the page the Cognito user pool signs users in on. Only set if a user pool was created.                                                                         |
