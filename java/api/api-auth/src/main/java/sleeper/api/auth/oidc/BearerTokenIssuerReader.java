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

import com.nimbusds.jwt.SignedJWT;

import java.util.Locale;
import java.util.Optional;

/**
 * Reads which issuer a token claims to come from, so the configuration to verify it with can be found.
 */
public class BearerTokenIssuerReader {

    private static final String BEARER_PREFIX = "bearer ";

    private BearerTokenIssuerReader() {
    }

    /**
     * Reads the token from an Authorization header. Anything that is not a bearer token is ignored, so that a request
     * signed with AWS credentials is not mistaken for one carrying a token.
     *
     * @param  authorizationHeader the value of the Authorization header
     * @return                     the token, or an empty optional if the header does not carry one
     */
    public static Optional<String> tokenFromHeader(String authorizationHeader) {
        if (authorizationHeader == null) {
            return Optional.empty();
        }
        String header = authorizationHeader.trim();
        if (!header.toLowerCase(Locale.ROOT).startsWith(BEARER_PREFIX)) {
            return Optional.empty();
        }
        String token = header.substring(BEARER_PREFIX.length()).trim();
        return token.isEmpty() ? Optional.empty() : Optional.of(token);
    }

    /**
     * Reads the issuer a token claims to come from, without verifying it.
     *
     * @param  token the token
     * @return       the issuer, or an empty optional if the token could not be read or does not name one
     */
    public static Optional<String> issuerOf(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            // Parsed as a signed token, so one with no signature never reaches an issuer's configuration.
            return Optional.ofNullable(SignedJWT.parse(token).getJWTClaimsSet().getIssuer())
                    .filter(issuer -> !issuer.isBlank());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Reads the issuer a request's token claims to come from, without verifying it.
     *
     * @param  authorizationHeader the value of the Authorization header
     * @return                     the issuer, or an empty optional if there is no readable token naming one
     */
    public static Optional<String> issuerOfHeader(String authorizationHeader) {
        return tokenFromHeader(authorizationHeader).flatMap(BearerTokenIssuerReader::issuerOf);
    }

}
