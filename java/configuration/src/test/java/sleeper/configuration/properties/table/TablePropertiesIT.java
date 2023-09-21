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
import sleeper.configuration.properties.instance.SystemDefinedInstanceProperty;

import static org.assertj.core.api.Assertions.assertThat;

class TablePropertiesIT extends TablePropertiesS3TestBase {

    @Test
    void shouldSaveToS3() {
        // Given
        TableProperties validProperties = createValidPropertiesWithTableNameAndBucket(
                "test", "save-properties");
        s3Client.createBucket("save-properties");

        // When
        validProperties.saveToS3(s3Client);

        // Then
        assertThat(s3Client.doesObjectExist("save-properties", "tables/test")).isTrue();
    }

    @Test
    void shouldLoadFromS3() {
        // Given
        TableProperties validProperties = createValidPropertiesWithTableNameAndBucket(
                "test", "load-properties");
        s3Client.createBucket("load-properties");
        validProperties.saveToS3(s3Client);

        // When
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(SystemDefinedInstanceProperty.CONFIG_BUCKET, "load-properties");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "test");

        // Then
        assertThat(tableProperties).isEqualTo(validProperties);
    }
}
