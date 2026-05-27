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

import sleeper.core.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DNS_SUFFIX;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;

public class DataFusionAwsConfigTest {

    @Test
    void shouldReturnConfigWithInstanceConfiguredEndpoint() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(REGION, "test-region");
        properties.set(DNS_SUFFIX, "test.domain.com");

        // When
        DataFusionAwsConfig config = DataFusionAwsConfig.getDefault(properties);

        // Then
        assertThat(config).isNotNull();
        assertThat(config.getEndpoint()).isEqualTo("https://s3.test-region.test.domain.com");
    }

    @Test
    void shouldReturnConfigWithEndpointProvided() {
        // Given
        String endpoint = "http://localhost:4566";

        // When
        DataFusionAwsConfig config = DataFusionAwsConfig.overrideEndpoint(endpoint);

        // Then
        assertThat(config).isNotNull();
        assertThat(config.getEndpoint()).isEqualTo("http://localhost:4566");
        assertThat(config.getRegion()).isEqualTo("us-east-1");
        assertThat(config.getAccessKeyId()).isEqualTo("test-access-key-id");
        assertThat(config.getSecretAccessKey()).isEqualTo("test-secret-access-key");
        assertThat(config.isAllowHttp()).isTrue();
    }
}
