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
package sleeper.foreign.datafusion;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DataFusionAwsConfigTest {

    @Test
    void shouldReturnConfigWhenEndpointArgumentProvided() {
        // Given
        String endpoint = "http://localhost:4566";

        // When
        DataFusionAwsConfig config = DataFusionAwsConfig.overrideEndpointFromEnv(endpoint, name -> null);

        // Then
        assertThat(config).isNotNull();
        assertThat(config.getEndpoint()).isEqualTo("http://localhost:4566");
        assertThat(config.getRegion()).isEqualTo("us-east-1");
        assertThat(config.getAccessKeyId()).isEqualTo("test-access-key-id");
        assertThat(config.getSecretAccessKey()).isEqualTo("test-secret-access-key");
        assertThat(config.isAllowHttp()).isTrue();
    }

    @Test
    void shouldReturnConfigFromEnvironmentWhenEndpointArgumentIsNull() {
        // Given
        Map<String, String> env = new HashMap<>();
        env.put("AWS_ENDPOINT_URL", "http://env-endpoint:4566");

        // When
        DataFusionAwsConfig config = DataFusionAwsConfig.overrideEndpointFromEnv(null, env::get);

        // Then
        assertThat(config).isNotNull();
        assertThat(config.getEndpoint()).isEqualTo("http://env-endpoint:4566");
        assertThat(config.getRegion()).isEqualTo("us-east-1");
        assertThat(config.getAccessKeyId()).isEqualTo("test-access-key-id");
        assertThat(config.getSecretAccessKey()).isEqualTo("test-secret-access-key");
        assertThat(config.isAllowHttp()).isTrue();
    }

    @Test
    void shouldPreferEndpointArgumentOverEnvironment() {
        // Given
        String endpoint = "http://argument-endpoint:4566";
        Map<String, String> env = new HashMap<>();
        env.put("AWS_ENDPOINT_URL", "http://env-endpoint:4566");

        // When
        DataFusionAwsConfig config = DataFusionAwsConfig.overrideEndpointFromEnv(endpoint, env::get);

        // Then
        assertThat(config).isNotNull();
        assertThat(config.getEndpoint()).isEqualTo("http://argument-endpoint:4566");
    }

    @Test
    void shouldReturnNullWhenBothEndpointArgumentAndEnvironmentAreNull() {
        // When
        DataFusionAwsConfig config = DataFusionAwsConfig.overrideEndpointFromEnv(null, name -> null);

        // Then
        assertThat(config).isNull();
    }

    @Test
    void shouldReturnNullWhenEnvironmentVariableNotSet() {
        // Given
        Map<String, String> env = new HashMap<>();

        // When
        DataFusionAwsConfig config = DataFusionAwsConfig.overrideEndpointFromEnv(null, env::get);

        // Then
        assertThat(config).isNull();
    }
}
