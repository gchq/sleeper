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

package sleeper.configuration.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;

import static org.assertj.core.api.Assertions.assertThat;

class TablePropertiesProviderIT extends TablePropertiesS3TestBase {

    @Test
    void shouldLoadFromS3() {
        // Given
        TableProperties validProperties = createValidPropertiesWithTableNameAndBucket(
                "test", "provider-load");
        s3Client.createBucket("provider-load");
        validProperties.saveToS3(s3Client);

        // When
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CdkDefinedInstanceProperty.CONFIG_BUCKET, "provider-load");
        TablePropertiesProvider provider = new TablePropertiesProvider(s3Client, instanceProperties);

        // Then
        assertThat(provider.getTableProperties("test")).isEqualTo(validProperties);
        assertThat(provider.getTablePropertiesIfExists("test")).contains(validProperties);
    }

    @Test
    void shouldReportTableDoesNotExistWhenNotInBucket() {
        // Given
        s3Client.createBucket("provider-no-table");

        // When
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(CdkDefinedInstanceProperty.CONFIG_BUCKET, "provider-no-table");
        TablePropertiesProvider provider = new TablePropertiesProvider(s3Client, instanceProperties);

        // Then
        assertThat(provider.getTablePropertiesIfExists("test"))
                .isEmpty();
    }
}
