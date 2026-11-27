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
package sleeper.api.auth;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OidcIssuerConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.properties.instance.ApiProperty.API_AUTH_COGNITO_ENABLED;
import static sleeper.core.properties.instance.ApiProperty.API_AUTH_IAM_ENABLED;
import static sleeper.core.properties.instance.ApiProperty.API_AUTH_OIDC_ISSUERS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_COGNITO_CLIENT_ID;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_COGNITO_ISSUER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_COGNITO_LOGIN_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_IAM_URL;

public class AuthSourceRegistryTest {

    private static final String ISSUER = "https://idp.internal/realms/sleeper";
    private static final String POOL_ISSUER = "https://cognito-idp.eu-west-1.amazonaws.com/eu-west-1_abc123";
    private static final String POOL_LOGIN_URL = "https://sleeper-test-api.auth.eu-west-1.amazoncognito.com";

    private final InstanceProperties properties = new InstanceProperties();

    @Nested
    @DisplayName("Report whether a client can authenticate at all")
    class ReportWhetherClientCanAuthenticate {

        @Test
        void shouldHaveNoWayToAuthenticateWhenNothingIsConfigured() {
            properties.set(API_AUTH_IAM_ENABLED, "false");

            assertThat(registry().isConfigured()).isFalse();
        }

        @Test
        void shouldAuthenticateWithAnOidcIssuer() {
            properties.set(API_AUTH_IAM_ENABLED, "false");
            setIssuers();

            assertThat(registry().isConfigured()).isTrue();
        }

        @Test
        void shouldAuthenticateWithIamAlone() {
            properties.set(API_AUTH_IAM_ENABLED, "true");
            deployIamEndpoint();

            assertThat(registry().isConfigured()).isTrue();
        }

        @Test
        void shouldNotAuthenticateWithIamBeforeTheEndpointHasBeenDeployed() {
            properties.set(API_AUTH_IAM_ENABLED, "true");

            assertThat(registry().isIamEnabled()).isFalse();
            assertThat(registry().isConfigured()).isFalse();
        }

        @Test
        void shouldNotReportIamOnceItIsSwitchedOffEvenBeforeTheEndpointIsRemoved() {
            properties.set(API_AUTH_IAM_ENABLED, "false");
            deployIamEndpoint();

            assertThat(registry().isIamEnabled()).isFalse();
        }

        @Test
        void shouldAuthenticateWithTheUserPoolAlone() {
            properties.set(API_AUTH_IAM_ENABLED, "false");
            deployUserPool();

            assertThat(registry().isConfigured()).isTrue();
        }

        @Test
        void shouldNotAuthenticateWithAUserPoolThatHasNotBeenDeployed() {
            // Asking for a pool does not create one, a deployment is required.
            properties.set(API_AUTH_IAM_ENABLED, "false");
            properties.set(API_AUTH_COGNITO_ENABLED, "true");

            assertThat(registry().isConfigured()).isFalse();
        }

        @Test
        void shouldAuthenticateWithIamByDefaultOnceDeployed() {
            deployIamEndpoint();

            assertThat(registry().isConfigured()).isTrue();
            assertThat(registry().isIamEnabled()).isTrue();
        }
    }

    @Nested
    @DisplayName("Find the issuer to verify a token with")
    class FindIssuerToVerifyWith {

        @Test
        void shouldFindAConfiguredIssuer() {
            setIssuers();

            assertThat(registry().forIssuer(ISSUER))
                    .map(OidcIssuerConfig::name)
                    .contains("corp");
        }

        @Test
        void shouldNotFindAnIssuerThatIsNotConfigured() {
            setIssuers();

            assertThat(registry().forIssuer("https://somewhere.else/realms/sleeper")).isEmpty();
        }

        @Test
        void shouldNotFindAnIssuerWhenNoneAreConfigured() {
            assertThat(registry().forIssuer(ISSUER)).isEmpty();
        }

        @Test
        void shouldNotFindAnIssuerForAMissingClaim() {
            setIssuers();

            assertThat(registry().forIssuer(null)).isEmpty();
        }

        @Test
        void shouldKeepIssuersInTheOrderTheyWereConfigured() {
            properties.set(API_AUTH_OIDC_ISSUERS, """
                    [{"name": "first", "issuer": "https://first.internal", "audiences": ["sleeper-api"],
                      "jwksUri": "https://first.internal/certs"},
                     {"name": "second", "issuer": "https://second.internal", "audiences": ["sleeper-api"],
                      "jwksUri": "https://second.internal/certs"}]
                    """);

            assertThat(registry().issuers())
                    .extracting(OidcIssuerConfig::name)
                    .containsExactly("first", "second");
        }
    }

    @Nested
    @DisplayName("Trust the user pool created for the instance")
    class TrustUserPool {

        @Test
        void shouldVerifyATokenFromTheUserPool() {
            deployUserPool();

            assertThat(registry().forIssuer(POOL_ISSUER))
                    .map(OidcIssuerConfig::name)
                    .contains(OidcIssuerConfig.COGNITO_ISSUER_NAME);
        }

        @Test
        void shouldOnlyAcceptATokenMeantForTheApplication() {
            deployUserPool();

            assertThat(registry().forIssuer(POOL_ISSUER))
                    .map(OidcIssuerConfig::audiences)
                    .contains(List.of("the-client-id"));
        }

        @Test
        void shouldRefuseATokenOfADifferentKindToTheOneIdentifyingWhoSignedIn() {
            deployUserPool();

            assertThat(registry().forIssuer(POOL_ISSUER))
                    .map(OidcIssuerConfig::requiredClaims)
                    .contains(Map.of("token_use", "id"));
        }

        @Test
        void shouldFetchThePoolsKeysRatherThanHoldThem() {
            deployUserPool();

            assertThat(registry().forIssuer(POOL_ISSUER)).hasValueSatisfying(issuer -> {
                assertThat(issuer.jwksUri()).isEqualTo(POOL_ISSUER + "/.well-known/jwks.json");
                assertThat(issuer.publicKey()).isNull();
            });
        }

        @Test
        void shouldTellABrowserWhereToSendSomeoneToSignIn() {
            deployUserPool();

            assertThat(registry().forIssuer(POOL_ISSUER)).hasValueSatisfying(issuer -> {
                assertThat(issuer.authEndpoint()).isEqualTo(POOL_LOGIN_URL + "/oauth2/authorize");
                assertThat(issuer.clientId()).isEqualTo("the-client-id");
            });
        }

        @Test
        void shouldTellABrowserWhereToSendSomeoneToEndTheirSessionWithThePool() {
            deployUserPool();

            assertThat(registry().forIssuer(POOL_ISSUER))
                    .map(OidcIssuerConfig::logoutEndpoint)
                    .contains(POOL_LOGIN_URL + "/logout");
        }

        @Test
        void shouldTrustNoPoolWhenNotDeployed() {
            deployUserPool();
            properties.set(API_AUTH_COGNITO_ENABLED, "false");

            assertThat(registry().forIssuer(POOL_ISSUER)).isEmpty();
        }

        @Test
        void shouldTrustNoPoolBeforeOneHasBeenDeployed() {
            // The instance is configured to have a pool, but it does not exist until the next deployment.
            properties.set(API_AUTH_COGNITO_ENABLED, "true");

            assertThat(registry().issuers()).isEmpty();
        }

        @Test
        void shouldTrustBothAConfiguredIssuerAndTheUserPool() {
            setIssuers();
            deployUserPool();

            assertThat(registry().issuers())
                    .extracting(OidcIssuerConfig::name)
                    .containsExactly("corp", OidcIssuerConfig.COGNITO_ISSUER_NAME);
        }

        @Test
        void shouldVerifyTokensFromTheUserPoolWithAConfiguredIssuerForThatPool() {
            properties.set(API_AUTH_OIDC_ISSUERS, """
                    [{"name": "pool-other-app", "issuer": "%s", "audiences": ["other-client-id"],
                      "jwksUri": "%s/.well-known/jwks.json"}]
                    """.formatted(POOL_ISSUER, POOL_ISSUER));
            deployUserPool();

            assertThat(registry().issuers())
                    .extracting(OidcIssuerConfig::name, OidcIssuerConfig::audiences, OidcIssuerConfig::requiredClaims)
                    .containsExactly(tuple("pool-other-app", List.of("other-client-id"), Map.of()));
        }

    }

    private AuthSourceRegistry registry() {
        return AuthSourceRegistry.from(properties);
    }

    private void deployUserPool() {
        properties.set(API_AUTH_COGNITO_ENABLED, "true");
        properties.set(REST_API_COGNITO_ISSUER, POOL_ISSUER);
        properties.set(REST_API_COGNITO_CLIENT_ID, "the-client-id");
        properties.set(REST_API_COGNITO_LOGIN_URL, POOL_LOGIN_URL);
    }

    private void deployIamEndpoint() {
        properties.set(REST_API_IAM_URL, "https://iam-api.execute-api.eu-west-2.amazonaws.com");
    }

    private void setIssuers() {
        properties.set(API_AUTH_OIDC_ISSUERS, """
                [{"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"],
                  "jwksUri": "https://idp.internal/certs"}]
                """.formatted(ISSUER));
    }

}
