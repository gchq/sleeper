/*
 * Copyright 2022-2026 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.List;

/**
 * Definitions of instance properties relating to the Sleeper REST API.
 */
public interface ApiProperty {
    UserDefinedInstanceProperty API_AUTH_OIDC_ISSUERS = Index.propertyBuilder("sleeper.rest.api.auth.oidc.issuers")
            .description("The OIDC issuers whose tokens the API will accept, as a JSON array.\n" +
                    "\n" +
                    "Each issuer must set a name, an issuer identifier matching the iss claim in its tokens, and at " +
                    "least one audience.\n" +
                    "\n" +
                    "Each issuer must also supply its signing keys, by setting either jwksUri for the API to fetch " +
                    "them from, or publicKey to hold them directly. Use publicKey where the API has no network " +
                    "access to the issuer.\n" +
                    "\n" +
                    "Optionally set principalClaim (defaults to sub) and clockSkewSeconds.\n" +
                    "\n" +
                    "To sign in from the UI, set authEndpoint, tokenEndpoint and clientId, and include the clientId " +
                    "in audiences. Optionally set logoutEndpoint, so that signing out of the UI also ends the " +
                    "session with the issuer.\n" +
                    "\n" +
                    "For example:\n" +
                    "[{\"name\":\"corp\",\"issuer\":\"https://idp.internal/realms/sleeper\"," +
                    "\"audiences\":[\"sleeper-api\"]," +
                    "\"jwksUri\":\"https://idp.internal/realms/sleeper/protocol/openid-connect/certs\"}]\n" +
                    "\n" +
                    "Configuring an issuer lets its users authenticate. It does not give them any access. Permissions " +
                    "come only from the principal allow-list.\n" +
                    "\n" +
                    "The name \"AWS Cognito\" is reserved for the Cognito user pool Sleeper can create, set by " +
                    "sleeper.rest.api.auth.cognito.enabled. An issuer with the same issuer identifier as that user " +
                    "pool replaces how its tokens are verified, including the built-in requirement for ID tokens.\n" +
                    "\n" +
                    "Set this on a single line. A backslash starts an escape sequence in a properties file, so avoid " +
                    "a key format that contains one.")
            .validationPredicate(SleeperPropertyValueUtils::isValidOidcIssuers)
            .propertyGroup(InstancePropertyGroup.API).build();
    UserDefinedInstanceProperty API_AUTH_ADMIN_PRINCIPALS = Index.propertyBuilder("sleeper.rest.api.auth.admin.principals")
            .description("A list of principals granted administrator access to the API. This is currently the only " +
                    "way to grant access to the API, and it grants full access, so keep it to as few principals as " +
                    "possible.\n" +
                    "\n" +
                    "A principal is written as iam#<role ARN> or jwt#<issuer>#<subject>. For example:\n" +
                    "iam#arn:aws:iam::123456789012:role/SleeperAdmin,jwt#https://idp.internal/realms/sleeper#a1b2c3")
            .validationPredicate(SleeperPropertyValueUtils::isValidPrincipalList)
            .propertyGroup(InstancePropertyGroup.API).build();
    UserDefinedInstanceProperty API_AUTH_IAM_ENABLED = Index.propertyBuilder("sleeper.rest.api.auth.iam.enabled")
            .description("Whether to deploy an API endpoint that authenticates clients by their IAM role, using " +
                    "AWS Signature Version 4.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.API)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty API_AUTH_COGNITO_ENABLED = Index.propertyBuilder("sleeper.rest.api.auth.cognito.enabled")
            .description("Whether to create a Cognito user pool to authenticate users of the API.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.API)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty API_CORS_ALLOWED_ORIGINS = Index.propertyBuilder("sleeper.rest.api.cors.allowed.origins")
            .description("A list of origins the API will accept browser requests from. This is empty by default, which means no cross-origin requests are allowed.")
            .propertyGroup(InstancePropertyGroup.API)
            .ignoreEmptyValue(false)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty API_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.rest.api.lambda.memory.mb")
            .description("The amount of memory in MB for the lambda function that serves the API.")
            .defaultValue("1024")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.API)
            .runCdkDeployWhenChanged(true).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
