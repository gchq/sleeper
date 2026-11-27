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
package sleeper.core.properties.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OidcIssuerConfigSerDeTest {

    private static final String JWKS_URI = "https://idp.internal/realms/sleeper/protocol/openid-connect/certs";
    private static final String ISSUER = "https://idp.internal/realms/sleeper";
    private static final String PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\\nMIIBIjAN\\n-----END PUBLIC KEY-----";

    @Nested
    @DisplayName("Read issuers")
    class ReadIssuers {

        @Test
        void shouldReadNoIssuersWhenPropertyIsNotSet() {
            assertThat(OidcIssuerConfigSerDe.readList(null)).isEmpty();
        }

        @Test
        void shouldReadNoIssuersWhenPropertyIsBlank() {
            assertThat(OidcIssuerConfigSerDe.readList("   ")).isEmpty();
        }

        @Test
        void shouldReadNoIssuersWhenArrayIsEmpty() {
            assertThat(OidcIssuerConfigSerDe.readList("[]")).isEmpty();
        }

        @Test
        void shouldReadIssuerWithKeysFetchedFromTheIssuer() {
            assertThat(OidcIssuerConfigSerDe.readList(json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s"}
                    """.formatted(ISSUER, JWKS_URI))))
                    .containsExactly(OidcIssuerConfig.builder()
                            .name("corp").issuer(ISSUER).audiences(List.of("sleeper-api")).jwksUri(JWKS_URI)
                            .build());
        }

        @Test
        void shouldReadIssuerWithKeyHeldInConfiguration() {
            assertThat(OidcIssuerConfigSerDe.readList(json("""
                    {"name": "isolated", "issuer": "%s", "audiences": ["sleeper-api"], "publicKey": "%s"}
                    """.formatted(ISSUER, PUBLIC_KEY))))
                    .containsExactly(OidcIssuerConfig.builder()
                            .name("isolated").issuer(ISSUER).audiences(List.of("sleeper-api"))
                            .publicKey(PUBLIC_KEY.replace("\\n", "\n"))
                            .build());
        }

        @Test
        void shouldReadKeySetHeldAsJsonRatherThanAsAString() {
            assertThat(OidcIssuerConfigSerDe.readList(json("""
                    {"name": "isolated", "issuer": "%s", "audiences": ["sleeper-api"],
                     "publicKey": {"keys": [{"kty": "RSA", "kid": "one", "n": "abc", "e": "AQAB"}]}}
                    """.formatted(ISSUER))))
                    .extracting(OidcIssuerConfig::publicKey)
                    .containsExactly("{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"one\",\"n\":\"abc\",\"e\":\"AQAB\"}]}");
        }

        @Test
        void shouldRefuseAKeySetTheApiCouldNotVerifyATokenAgainst() {
            // A key held in the configuration is read as RSA, so an issuer signing with an elliptic curve key has to
            // be reachable instead.
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json("""
                    {"name": "isolated", "issuer": "%s", "audiences": ["sleeper-api"],
                     "publicKey": {"keys": [{"kty": "EC", "kid": "one", "crv": "P-256"}]}}
                    """.formatted(ISSUER))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("RSA")
                    .hasMessageContaining("jwksUri");
        }

        @Test
        void shouldRefuseAKeySetThatHoldsNoKeys() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json("""
                    {"name": "isolated", "issuer": "%s", "audiences": ["sleeper-api"],
                     "publicKey": {"notKeys": []}}
                    """.formatted(ISSUER))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JSON Web Key Set");
        }

        @Test
        void shouldRefuseAKeySetWithAnEmptyListOfKeys() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json("""
                    {"name": "isolated", "issuer": "%s", "audiences": ["sleeper-api"],
                     "publicKey": {"keys": []}}
                    """.formatted(ISSUER))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least one key");
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "   "})
        void shouldRefuseAnEmptyKey(String key) {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json("""
                    {"name": "isolated", "issuer": "%s", "audiences": ["sleeper-api"], "publicKey": "%s"}
                    """.formatted(ISSUER, key))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty publicKey");
        }

        @Test
        void shouldWriteKeySetBackAsJsonSoThatItIsNotTurnedIntoAString() {
            String json = json("""
                    {"name": "isolated", "issuer": "%s", "audiences": ["sleeper-api"],
                     "publicKey": {"keys": [{"kty": "RSA", "kid": "one", "n": "abc", "e": "AQAB"}]}}
                    """.formatted(ISSUER));

            assertThat(OidcIssuerConfigSerDe.toJson(OidcIssuerConfigSerDe.readList(json)))
                    .contains("\"publicKey\":{\"keys\":[")
                    .satisfies(written -> assertThat(OidcIssuerConfigSerDe.readList(written))
                            .isEqualTo(OidcIssuerConfigSerDe.readList(json)));
        }

        @Test
        void shouldReadMultipleIssuersInTheOrderTheyWereDeclared() {
            assertThat(OidcIssuerConfigSerDe.readList(json(
                    issuerJson("first", "https://first.internal"),
                    issuerJson("second", "https://second.internal"))))
                    .extracting(OidcIssuerConfig::name)
                    .containsExactly("first", "second");
        }

        @Test
        void shouldReadAllOptionalFields() {
            assertThat(OidcIssuerConfigSerDe.readList(json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api", "sleeper-ui"],
                     "jwksUri": "%s", "tokenEndpoint": "https://idp.internal/token",
                     "principalClaim": "preferred_username", "clockSkewSeconds": 120}
                    """.formatted(ISSUER, JWKS_URI))))
                    .containsExactly(OidcIssuerConfig.builder()
                            .name("corp").issuer(ISSUER)
                            .audiences(List.of("sleeper-api", "sleeper-ui"))
                            .jwksUri(JWKS_URI)
                            .tokenEndpoint("https://idp.internal/token")
                            .principalClaim("preferred_username")
                            .clockSkewSeconds(120)
                            .build());
        }

        @Test
        void shouldReadTheDetailsABrowserNeedsToSendSomeoneToSignIn() {
            assertThat(OidcIssuerConfigSerDe.readList(json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api", "sleeper-ui"], "jwksUri": "%s",
                     "authEndpoint": "https://idp.internal/authorize", "logoutEndpoint": "https://idp.internal/logout",
                     "clientId": "sleeper-ui"}
                    """.formatted(ISSUER, JWKS_URI))))
                    .containsExactly(OidcIssuerConfig.builder()
                            .name("corp").issuer(ISSUER).audiences(List.of("sleeper-api", "sleeper-ui")).jwksUri(JWKS_URI)
                            .authEndpoint("https://idp.internal/authorize")
                            .logoutEndpoint("https://idp.internal/logout")
                            .clientId("sleeper-ui")
                            .build());
        }

        @Test
        void shouldRefuseAClientIdForSigningInThatIsNotOneOfTheAudiences() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s",
                     "authEndpoint": "https://idp.internal/authorize", "clientId": "sleeper-ui"}
                    """.formatted(ISSUER, JWKS_URI))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("clientId \"sleeper-ui\"")
                    .hasMessageContaining("audiences");
        }

        @Test
        void shouldReadClaimsATokenMustCarryBeyondTheUsualOnes() {
            assertThat(OidcIssuerConfigSerDe.readList(json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s",
                     "requiredClaims": {"token_use": "id"}}
                    """.formatted(ISSUER, JWKS_URI))))
                    .extracting(OidcIssuerConfig::requiredClaims)
                    .containsExactly(Map.of("token_use", "id"));
        }

        @Test
        void shouldRequireNoFurtherClaimsWhenNoneAreGiven() {
            assertThat(readOne(issuerJson("corp", ISSUER)).requiredClaims()).isEmpty();
        }

        @Test
        void shouldRefuseARequiredClaimThatIsNotAString() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s",
                     "requiredClaims": {"token_use": ["id"]}}
                    """.formatted(ISSUER, JWKS_URI))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("requiredClaims");
        }

        @Test
        void shouldWriteBackEveryFieldItRead() {
            String json = json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-ui"], "jwksUri": "%s",
                     "tokenEndpoint": "https://idp.internal/token",
                     "authEndpoint": "https://idp.internal/authorize", "clientId": "sleeper-ui",
                     "logoutEndpoint": "https://idp.internal/logout",
                     "requiredClaims": {"token_use": "id"},
                     "principalClaim": "preferred_username", "clockSkewSeconds": 120}
                    """.formatted(ISSUER, JWKS_URI));

            assertThat(OidcIssuerConfigSerDe.readList(OidcIssuerConfigSerDe.toJson(
                    OidcIssuerConfigSerDe.readList(json))))
                    .isEqualTo(OidcIssuerConfigSerDe.readList(json));
        }

        @Test
        void shouldIgnoreFieldsThisVersionDoesNotRecognise() {
            // An instance running an older version of Sleeper must not reject configuration written by a newer
            // version, as that would deny all access to the API.
            assertThat(OidcIssuerConfigSerDe.readList(json("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s",
                     "somethingAddedInALaterVersion": {"nested": true}}
                    """.formatted(ISSUER, JWKS_URI))))
                    .extracting(OidcIssuerConfig::name)
                    .containsExactly("corp");
        }
    }

    @Nested
    @DisplayName("Apply defaults")
    class ApplyDefaults {

        @Test
        void shouldIdentifyTheCallerBySubjectByDefault() {
            assertThat(readOne(issuerJson("corp", ISSUER)).principalClaim())
                    .isEqualTo("sub");
        }


        @Test
        void shouldApplyDefaultClockSkew() {
            assertThat(readOne(issuerJson("corp", ISSUER)).clockSkewSeconds())
                    .isEqualTo(60);
        }

        @Test
        void shouldNotSetATokenEndpointByDefault() {
            assertThat(readOne(issuerJson("corp", ISSUER)).tokenEndpoint())
                    .isNull();
        }
    }


    @Nested
    @DisplayName("Reject configuration that would accept the wrong tokens")
    class RejectUnsafeConfiguration {

        @Test
        void shouldRefuseIssuerWithNoAudience() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "jwksUri": "%s"}
                    """.formatted(ISSUER, JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("audience");
        }

        @Test
        void shouldRefuseIssuerWithEmptyListOfAudiences() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": [], "jwksUri": "%s"}
                    """.formatted(ISSUER, JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("audience");
        }

        @ParameterizedTest
        @CsvSource(delimiter = '|', value = {
            "tokenEndpoint|http://idp.internal/token",
            "authEndpoint|http://idp.internal/authorize",
            "logoutEndpoint|http://idp.internal/logout",
            "authEndpoint|javascript:alert(document.cookie)",
            "logoutEndpoint|data:text/html;base64,PHNjcmlwdD4="})
        void shouldRefuseAnEndpointThatDoesNotUseHttps(String field, String value) {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s", "%s": "%s"}
                    """.formatted(ISSUER, JWKS_URI, field, value)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("https for " + field);
        }

        @Test
        void shouldRefuseKeysFetchedOverAnUnencryptedConnection() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"],
                     "jwksUri": "http://idp.internal/certs"}
                    """.formatted(ISSUER)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("https");
        }

        @Test
        void shouldRefuseIssuerWithNoSigningKeys() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"]}
                    """.formatted(ISSUER)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("jwksUri or publicKey");
        }

        @Test
        void shouldRefuseIssuerWithTwoSourcesOfSigningKeys() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"],
                     "jwksUri": "%s", "publicKey": "%s"}
                    """.formatted(ISSUER, JWKS_URI, PUBLIC_KEY)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("only one of");
        }

        @Test
        void shouldRefuseDuplicateIssuerIdentifier() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json(
                    issuerJson("first", ISSUER),
                    issuerJson("second", ISSUER))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unique issuer identifier");
        }

        @Test
        void shouldRefuseDuplicateName() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json(
                    issuerJson("corp", "https://first.internal"),
                    issuerJson("corp", "https://second.internal"))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unique name");
        }

        @ParameterizedTest
        @ValueSource(strings = {"AWS Cognito", "aws cognito", " AWS Cognito "})
        void shouldRefuseTheNameReservedForTheUserPool(String name) {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(json(
                    issuerJson(name, ISSUER))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("reserved");
        }

        @Test
        void shouldRefuseClockSkewBeyondTheMaximum() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s",
                     "clockSkewSeconds": 301}
                    """.formatted(ISSUER, JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("clockSkewSeconds");
        }

        @Test
        void shouldRefuseNegativeClockSkew() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s",
                     "clockSkewSeconds": -1}
                    """.formatted(ISSUER, JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("clockSkewSeconds");
        }
    }

    @Nested
    @DisplayName("Reject malformed configuration")
    class RejectMalformedConfiguration {

        @Test
        void shouldRefuseValueThatIsNotJson() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList("not json at all"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldRefuseValueThatIsNotAnArray() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList(issuerJson("corp", ISSUER)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JSON array");
        }

        @Test
        void shouldRefuseEntryThatIsNotAnObject() {
            assertThatThrownBy(() -> OidcIssuerConfigSerDe.readList("[\"corp\"]"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JSON object");
        }

        @Test
        void shouldRefuseIssuerWithNoName() {
            assertThatThrownBy(() -> readOne("""
                    {"issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s"}
                    """.formatted(ISSUER, JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("name");
        }

        @Test
        void shouldRefuseIssuerWithNoIssuerIdentifier() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "audiences": ["sleeper-api"], "jwksUri": "%s"}
                    """.formatted(JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("issuer");
        }

        @Test
        void shouldRefuseIssuerIdentifierThatIsNotAbsolute() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "/realms/sleeper", "audiences": ["sleeper-api"], "jwksUri": "%s"}
                    """.formatted(JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("absolute URI");
        }

        @Test
        void shouldRefuseAudiencesThatAreNotAList() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": "sleeper-api", "jwksUri": "%s"}
                    """.formatted(ISSUER, JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("array of strings");
        }

        @Test
        void shouldRefuseClockSkewThatIsNotANumber() {
            assertThatThrownBy(() -> readOne("""
                    {"name": "corp", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s",
                     "clockSkewSeconds": "sixty"}
                    """.formatted(ISSUER, JWKS_URI)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("number");
        }
    }

    @Nested
    @DisplayName("Write issuers")
    class WriteIssuers {

        @Test
        void shouldWriteNoIssuers() {
            assertThat(OidcIssuerConfigSerDe.toJson(List.of())).isEqualTo("[]");
        }

        @Test
        void shouldReadBackIssuersThatWereWritten() {
            List<OidcIssuerConfig> issuers = List.of(
                    OidcIssuerConfig.builder()
                            .name("corp").issuer(ISSUER).audiences(List.of("sleeper-api")).jwksUri(JWKS_URI)
                            .tokenEndpoint("https://idp.internal/token")
                            .build(),
                    OidcIssuerConfig.builder()
                            .name("isolated").issuer("https://other.internal").audiences(List.of("sleeper-api"))
                            .publicKey("a-public-key").principalClaim("email").clockSkewSeconds(5)
                            .build());

            assertThat(OidcIssuerConfigSerDe.readList(OidcIssuerConfigSerDe.toJson(issuers)))
                    .isEqualTo(issuers);
        }
    }

    private static OidcIssuerConfig readOne(String issuerJson) {
        return OidcIssuerConfigSerDe.readList(json(issuerJson)).get(0);
    }

    private static String json(String... issuers) {
        return "[" + String.join(",", issuers) + "]";
    }

    private static String issuerJson(String name, String issuer) {
        return """
                {"name": "%s", "issuer": "%s", "audiences": ["sleeper-api"], "jwksUri": "%s"}
                """.formatted(name, issuer, JWKS_URI);
    }
}
