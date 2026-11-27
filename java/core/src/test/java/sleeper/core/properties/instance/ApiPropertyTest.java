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
package sleeper.core.properties.instance;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.SleeperPropertiesInvalidException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.ApiProperty.API_AUTH_OIDC_ISSUERS;
import static sleeper.core.properties.instance.ApiProperty.API_CORS_ALLOWED_ORIGINS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class ApiPropertyTest {

    private final InstanceProperties properties = createTestInstanceProperties();

    @Test
    void shouldAcceptAnInstanceThatConfiguresNoOidcIssuers() {
        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptAValidOidcIssuer() {
        properties.set(API_AUTH_OIDC_ISSUERS, """
                [{"name": "corp", "issuer": "https://idp.internal/realms/sleeper",
                  "audiences": ["sleeper-api"], "jwksUri": "https://idp.internal/certs"}]
                """);

        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectAnInvalidOidcIssuer() {
        properties.set(API_AUTH_OIDC_ISSUERS, """
                [{"name": "corp", "issuer": "https://idp.internal/realms/sleeper", "jwksUri": "https://idp.internal/certs"}]
                """);

        assertThatThrownBy(properties::validate)
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining("sleeper.rest.api.auth.oidc.issuers");
    }

    @Test
    void shouldTreatAnEmptyListOfAllowedOriginsAsAllowingNone() {
        properties.set(API_CORS_ALLOWED_ORIGINS, "");
        assertThat(properties.getList(API_CORS_ALLOWED_ORIGINS)).isEmpty();
    }

}
