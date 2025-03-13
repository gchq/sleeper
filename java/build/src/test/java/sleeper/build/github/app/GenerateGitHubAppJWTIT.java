/*
 * Copyright 2022-2025 Crown Copyright
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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.impl.FixedClock;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.sql.Date;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class GenerateGitHubAppJWTIT {
    @TempDir
    private Path tempDir;

    @Test
    void shouldGenerateJWT() {
        // Given
        KeyPair keyPair = Jwts.SIG.RS256.keyPair().build();
        Instant time = Instant.parse("2023-11-23T15:24:00Z");
        Instant expectedIssuedAt = Instant.parse("2023-11-23T15:23:00Z");
        Instant expectedExpiry = Instant.parse("2023-11-23T15:34:00Z");

        // When
        String jwt = GenerateGitHubAppJWT.withKeyAndAppIdAtTime(keyPair.getPrivate(), "test-app-id", time);

        // Then
        Jws<Claims> found = Jwts.parser()
                .clock(new FixedClock(Date.from(time)))
                .verifyWith(keyPair.getPublic())
                .build().parse(jwt).accept(Jws.CLAIMS);
        assertThat(found)
                .extracting(Jws::getHeader, Jws::getPayload)
                .containsExactly(
                        Map.of("alg", "RS256"),
                        Map.of("iss", "test-app-id",
                                "iat", expectedIssuedAt.getEpochSecond(),
                                "exp", expectedExpiry.getEpochSecond()));
    }

    @Test
    void shouldGenerateJWTFromPemFile() throws Exception {
        // Given
        KeyPair keyPair = Jwts.SIG.RS256.keyPair().build();
        writePEMFile(keyPair, tempDir.resolve("private.pem"));

        // When
        String jwt = GenerateGitHubAppJWT.withPemFileAndAppId(
                tempDir.resolve("private.pem"),
                "test-app-id");

        // Then
        assertThat(jwt).isNotBlank();
    }

    private static void writePEMFile(KeyPair keyPair, Path path) throws IOException {
        StringWriter stringWriter = new StringWriter();
        JcaPEMWriter w = new JcaPEMWriter(stringWriter);
        w.writeObject(keyPair);
        w.flush();
        Files.writeString(path, stringWriter.toString());
    }
}
