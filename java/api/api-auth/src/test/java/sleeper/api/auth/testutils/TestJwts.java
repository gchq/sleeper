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
package sleeper.api.auth.testutils;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;

import java.time.Instant;
import java.util.Date;
import java.util.stream.Stream;

/**
 * Mints tokens for tests, signed with a key generated for the test rather than one checked into the repository.
 */
public class TestJwts {

    private final RSAKey key;

    private TestJwts(RSAKey key) {
        this.key = key;
    }

    /**
     * Creates an issuer with a freshly generated signing key.
     *
     * @return                       the issuer
     * @throws IllegalStateException if a key could not be generated
     */
    public static TestJwts issuer() {
        return issuerWithKeyId("test-key");
    }

    /**
     * Creates an issuer with a freshly generated signing key, identified by the given ID. Used to check that a token
     * is verified against the key it names rather than any key the issuer happens to publish.
     *
     * @param  keyId                 the ID to publish the key under
     * @return                       the issuer
     * @throws IllegalStateException if a key could not be generated
     */
    public static TestJwts issuerWithKeyId(String keyId) {
        try {
            return new TestJwts(new RSAKeyGenerator(2048).keyID(keyId).generate());
        } catch (Exception e) {
            throw new IllegalStateException("Could not generate a signing key for the test", e);
        }
    }

    /**
     * Publishes this issuer's signing key as a JSON Web Key Set, as it would be held in an instance's configuration.
     * Only the public half is published.
     *
     * @return the key set
     */
    public String publicJwks() {
        return new JWKSet(key.toPublicJWK()).toString();
    }

    /**
     * Publishes the signing keys of several issuers as one JSON Web Key Set, as an issuer part way through rotating
     * its keys would.
     *
     * @param  issuers the issuers
     * @return         the key set
     */
    public static String publicJwks(TestJwts... issuers) {
        return new JWKSet(Stream.of(issuers)
                .<JWK>map(issuer -> issuer.key.toPublicJWK())
                .toList()).toString();
    }

    /**
     * Mints a token signed by this issuer.
     *
     * @param  issuer  the issuer to claim the token came from
     * @param  subject the subject to identify the client by
     * @return         the token
     */
    public String token(String issuer, String subject) {
        return token(issuer, subject, claims -> claims);
    }

    /**
     * Mints a token signed by this issuer, meant for the given audience.
     *
     * @param  issuer   the issuer to claim the token came from
     * @param  subject  the subject to identify the client by
     * @param  audience the audience the token is meant for
     * @return          the token
     */
    public String tokenForAudience(String issuer, String subject, String audience) {
        return token(issuer, subject, claims -> claims.audience(audience));
    }

    /**
     * Mints a token signed by this issuer, adjusting its claims.
     *
     * @param  issuer    the issuer to claim the token came from
     * @param  subject   the subject to identify the client by
     * @param  customise adjusts the claims before the token is signed
     * @return           the token
     */
    public String token(String issuer, String subject, ClaimsCustomiser customise) {
        try {
            JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder()
                    .issuer(issuer)
                    .subject(subject)
                    .issueTime(Date.from(Instant.now()))
                    .expirationTime(Date.from(Instant.now().plusSeconds(300)));
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(),
                    customise.customise(claims).build());
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException("Could not mint a token for the test", e);
        }
    }

    /**
     * Mints a token that does not name the key that signed it, as an issuer publishing only one key may.
     *
     * @param  issuer  the issuer to claim the token came from
     * @param  subject the subject to identify the client by
     * @return         the token
     */
    public String tokenNamingNoKey(String issuer, String subject) {
        try {
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).build(),
                    new JWTClaimsSet.Builder().issuer(issuer).subject(subject).build());
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException("Could not mint a token for the test", e);
        }
    }

    /**
     * Mints a token with no signature at all.
     *
     * @param  issuer the issuer to claim the token came from
     * @return        the token
     */
    public static String unsignedToken(String issuer) {
        return new PlainJWT(new JWTClaimsSet.Builder().issuer(issuer).subject("some-subject").build()).serialize();
    }

    /**
     * Adjusts the claims of a token before it is signed.
     */
    @FunctionalInterface
    public interface ClaimsCustomiser {
        /**
         * Adjusts the claims.
         *
         * @param  claims the claims so far
         * @return        the claims to sign
         */
        JWTClaimsSet.Builder customise(JWTClaimsSet.Builder claims);
    }

}
