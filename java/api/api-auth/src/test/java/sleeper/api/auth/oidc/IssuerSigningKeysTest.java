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

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import org.junit.jupiter.api.Test;

import sleeper.api.auth.testutils.TestJwts;

import java.util.Optional;

import static com.nimbusds.jose.jwk.Curve.P_256;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks which of an issuer's keys a token is verified against, for an issuer whose keys are held in the instance
 * configuration because it cannot be reached over the network.
 */
class IssuerSigningKeysTest {

    private static final String ISSUER = "https://idp.internal/realms/sleeper";
    private static final String SUBJECT = "a1b2c3";

    @Test
    void shouldPickTheKeyTheTokenWasSignedWithWhileTheIssuerIsRotatingThem() {
        TestJwts oldKey = TestJwts.issuerWithKeyId("old");
        TestJwts newKey = TestJwts.issuerWithKeyId("new");
        String keySet = TestJwts.publicJwks(oldKey, newKey);

        assertThat(keyFor(keySet, oldKey.token(ISSUER, SUBJECT)))
                .isNotEqualTo(keyFor(keySet, newKey.token(ISSUER, SUBJECT)))
                .isEqualTo(keyFor(oldKey.publicJwks(), oldKey.token(ISSUER, SUBJECT)));
    }

    @Test
    void shouldFindNoKeyWhenTheIssuerNoLongerPublishesTheOneTheTokenNames() {
        TestJwts retired = TestJwts.issuerWithKeyId("retired");
        TestJwts current = TestJwts.issuerWithKeyId("current");

        assertThat(keyFor(current.publicJwks(), retired.token(ISSUER, SUBJECT))).isEmpty();
    }

    @Test
    void shouldUseTheOnlyKeyWhenTheTokenNamesNone() {
        TestJwts issuer = TestJwts.issuer();

        assertThat(keyFor(issuer.publicJwks(), issuer.tokenNamingNoKey(ISSUER, SUBJECT))).isNotEmpty();
    }

    @Test
    void shouldFindNoKeyWhenTheTokenNamesNoneAndTheIssuerPublishesSeveral() {
        TestJwts issuer = TestJwts.issuerWithKeyId("one");
        String keySet = TestJwts.publicJwks(issuer, TestJwts.issuerWithKeyId("two"));

        assertThat(keyFor(keySet, issuer.tokenNamingNoKey(ISSUER, SUBJECT))).isEmpty();
    }

    @Test
    void shouldUseAConfiguredSingleKeyAsItIsWhenItIsNotAKeySet() {
        assertThat(keyFor("MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8", TestJwts.issuer().token(ISSUER, SUBJECT)))
                .contains("MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8");
    }

    @Test
    void shouldFindNoKeyForATokenThatCannotBeRead() {
        assertThat(keyFor(TestJwts.issuer().publicJwks(), "not-a-token")).isEmpty();
    }

    @Test
    void shouldFindNoKeyForAnIssuerSigningWithACurveThatCannotBeVerifiedLocally() throws Exception {
        // Keys held in the configuration are read as RSA, so an issuer signing with an elliptic curve key must be
        // reachable over the network for its keys to be fetched instead.
        String keySet = new JWKSet(new ECKeyGenerator(P_256).keyID("ec").generate().toPublicJWK()).toString();

        assertThat(keyFor(keySet, TestJwts.issuerWithKeyId("ec").token(ISSUER, SUBJECT))).isEmpty();
    }

    private static Optional<String> keyFor(String keySet, String token) {
        return IssuerSigningKeys.encodedKeyForToken(keySet, token);
    }

}
