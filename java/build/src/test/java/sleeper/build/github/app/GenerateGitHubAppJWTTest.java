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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.impl.FixedClock;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.sql.Date;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class GenerateGitHubAppJWTTest {

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
}
