/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.build.github.app;

import io.jsonwebtoken.Jwts;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.sql.Date;
import java.time.Instant;

public class GenerateGitHubAppJWT {

    public static String withPemFileAndAppId(Path pemFile, String appId) throws IOException {
        KeyPair keyPair;
        try (Reader reader = Files.newBufferedReader(pemFile)) {
            keyPair = readPrivatePem(reader);
        }
        return withKeyAndAppIdAtTime(keyPair.getPrivate(), appId, Instant.now());
    }

    public static String withKeyAndAppIdAtTime(PrivateKey key, String appId, Instant now) {
        return Jwts.builder()
                .signWith(key)
                .claim("iat", Date.from(now.minusSeconds(60)))
                .claim("exp", Date.from(now.plusSeconds(10 * 60)))
                .claim("iss", appId)
                .compact();
    }

    private static KeyPair readPrivatePem(Reader reader) throws IOException {
        PEMParser parser = new PEMParser(reader);
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        return converter.getKeyPair((PEMKeyPair) parser.readObject());
    }
}
