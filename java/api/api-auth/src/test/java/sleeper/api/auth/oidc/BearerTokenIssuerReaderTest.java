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
package sleeper.api.auth.oidc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import sleeper.api.auth.testutils.TestJwts;

import static org.assertj.core.api.Assertions.assertThat;

public class BearerTokenIssuerReaderTest {

    private static final String ISSUER = "https://idp.internal/realms/sleeper";

    private final TestJwts issuer = TestJwts.issuer();

    @Nested
    @DisplayName("Read the token from a request")
    class ReadTokenFromRequest {

        @Test
        void shouldReadABearerToken() {
            assertThat(BearerTokenIssuerReader.tokenFromHeader("Bearer some-token")).contains("some-token");
        }

        @Test
        void shouldReadABearerTokenHoweverTheSchemeIsCapitalised() {
            assertThat(BearerTokenIssuerReader.tokenFromHeader("bEaReR some-token")).contains("some-token");
        }

        @Test
        void shouldIgnoreARequestSignedWithAwsCredentials() {
            assertThat(BearerTokenIssuerReader.tokenFromHeader(
                    "AWS4-HMAC-SHA256 Credential=AKIA.../20260101/eu-west-2/execute-api/aws4_request"))
                    .isEmpty();
        }

        @ParameterizedTest
        @ValueSource(strings = {"Basic dXNlcjpwYXNz", "Bearer", "Bearer   ", "", "   ", "some-token"})
        void shouldIgnoreAHeaderThatDoesNotCarryABearerToken(String header) {
            assertThat(BearerTokenIssuerReader.tokenFromHeader(header)).isEmpty();
        }

        @Test
        void shouldIgnoreAMissingHeader() {
            assertThat(BearerTokenIssuerReader.tokenFromHeader(null)).isEmpty();
        }
    }

    @Nested
    @DisplayName("Read the issuer a token claims to come from")
    class ReadIssuerFromToken {

        @Test
        void shouldReadTheIssuer() {
            assertThat(BearerTokenIssuerReader.issuerOf(issuer.token(ISSUER, "some-subject"))).contains(ISSUER);
        }

        @Test
        void shouldReadTheIssuerFromARequest() {
            assertThat(BearerTokenIssuerReader.issuerOfHeader("Bearer " + issuer.token(ISSUER, "some-subject")))
                    .contains(ISSUER);
        }

        @Test
        void shouldIgnoreATokenWithNoSignature() {
            assertThat(BearerTokenIssuerReader.issuerOf(TestJwts.unsignedToken(ISSUER))).isEmpty();
        }

        @Test
        void shouldIgnoreATokenThatNamesNoIssuer() {
            assertThat(BearerTokenIssuerReader.issuerOf(issuer.token(null, "some-subject"))).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(strings = {"not a token", "a.b.c", "", "   ", "eyJhbGciOiJSUzI1NiJ9"})
        void shouldIgnoreAnythingThatIsNotAToken(String token) {
            assertThat(BearerTokenIssuerReader.issuerOf(token)).isEmpty();
        }

        @Test
        void shouldIgnoreAMissingToken() {
            assertThat(BearerTokenIssuerReader.issuerOf(null)).isEmpty();
        }
    }

}
