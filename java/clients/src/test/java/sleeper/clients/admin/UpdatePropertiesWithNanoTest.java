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
package sleeper.clients.admin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.PropertiesDiffTestHelper.valueChanged;
import static sleeper.clients.deploy.GeneratePropertiesTestHelper.generateTestInstanceProperties;
import static sleeper.clients.deploy.GeneratePropertiesTestHelper.generateTestTableProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

class UpdatePropertiesWithNanoTest {

    @TempDir
    private Path tempDir;
    private UpdatePropertiesWithNanoTestHelper helper;

    @BeforeEach
    void setUp() {
        helper = new UpdatePropertiesWithNanoTestHelper(tempDir);
    }

    @Test
    void shouldInvokeNanoOnInstancePropertiesFile() throws Exception {
        // Given
        InstanceProperties properties = generateTestInstanceProperties();

        // When / Then
        assertThat(helper.updateInstancePropertiesGetCommandRun(properties))
                .containsExactly("nano", tempDir.resolve("sleeper/admin/temp.properties").toString());
    }

    @Test
    void shouldWriteInstancePropertiesFile() throws Exception {
        // Given
        InstanceProperties properties = generateTestInstanceProperties();

        // When / Then
        assertThat(helper.updateInstancePropertiesGetPropertiesWritten(properties))
                .isEqualTo(properties);
    }

    @Test
    void shouldGetDiffAfterPropertiesChanged() throws Exception {
        // Given
        InstanceProperties before = generateTestInstanceProperties();
        before.set(INGEST_SOURCE_BUCKET, "bucket-before");
        InstanceProperties after = generateTestInstanceProperties();
        after.set(INGEST_SOURCE_BUCKET, "bucket-after");

        // When / Then
        assertThat(helper.updateProperties(before, after).getDiff())
                .extracting(PropertiesDiff::getChanges).asList()
                .containsExactly(valueChanged(INGEST_SOURCE_BUCKET, "bucket-before", "bucket-after"));
    }

    @Test
    void shouldRetrievePropertiesAfterChange() throws Exception {
        // Given
        InstanceProperties before = generateTestInstanceProperties();
        InstanceProperties after = generateTestInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");

        // When
        Properties properties = helper.updateProperties(before, after).getUpdatedProperties();

        // Then
        assertThat(new InstanceProperties(properties)).isEqualTo(after);
    }

    @Test
    void shouldUpdateTableProperties() throws Exception {
        // Given
        TableProperties before = generateTestTableProperties();
        before.set(ROW_GROUP_SIZE, "123");
        TableProperties after = generateTestTableProperties();
        after.set(ROW_GROUP_SIZE, "456");

        // When / Then
        assertThat(helper.updateProperties(before, after).getDiff())
                .extracting(PropertiesDiff::getChanges).asList()
                .containsExactly(valueChanged(ROW_GROUP_SIZE, "123", "456"));
    }
}
