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

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.SignedJWT;

import java.util.Base64;
import java.util.List;
import java.util.Optional;

/**
 * Picks which of an issuer's signing keys a token should be verified against.
 */
public class IssuerSigningKeys {

    private IssuerSigningKeys() {
    }

    /**
     * Finds the key a token was signed with, encoded as the API's token verification expects.
     *
     * @param  configuredKey the issuer's keys, as a JSON Web Key Set, or a single already encoded key
     * @param  token         the token to be verified
     * @return               the key, or an empty optional if no configured key could have signed the token
     */
    public static Optional<String> encodedKeyForToken(String configuredKey, String token) {
        Optional<List<JWK>> keySet = readKeySet(configuredKey);
        if (keySet.isEmpty()) {
            // Not a key set, so it is a single key already in the form token verification reads.
            return Optional.of(configuredKey);
        }
        return keyForToken(keySet.get(), token).flatMap(IssuerSigningKeys::encode);
    }

    private static Optional<List<JWK>> readKeySet(String configuredKey) {
        try {
            return Optional.of(JWKSet.parse(configuredKey).getKeys());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<JWK> keyForToken(List<JWK> keys, String token) {
        String keyId;
        try {
            keyId = SignedJWT.parse(token).getHeader().getKeyID();
        } catch (Exception e) {
            return Optional.empty();
        }
        if (keyId != null) {
            return keys.stream().filter(key -> keyId.equals(key.getKeyID())).findFirst();
        }
        return keys.size() == 1 ? Optional.of(keys.get(0)) : Optional.empty();
    }

    /**
     * Encodes a key for the API's token verification, which reads a single RSA public key rather than a key set.
     *
     * @param  key the key
     * @return     the key as base64 encoded DER, or an empty optional if it is not an RSA public key
     */
    private static Optional<String> encode(JWK key) {
        try {
            return Optional.of(Base64.getEncoder().encodeToString(key.toRSAKey().toRSAPublicKey().getEncoded()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}
