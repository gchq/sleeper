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
package sleeper.clients.admin.properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertyGroup;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static sleeper.clients.admin.properties.PropertiesDiffTestHelper.valueChanged;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class UpdatePropertiesWithTextEditorIT {

    @TempDir
    private Path tempDir;
    private UpdatePropertiesWithTextEditorTestHelper helper;
    private String id = "test-instance";

    @BeforeEach
    void setUp() {
        helper = new UpdatePropertiesWithTextEditorTestHelper(tempDir);
    }

    @Nested
    @DisplayName("Open instance properties file")
    class OpenInstanceProperties {

        @Test
        void shouldInvokeEditorOnInstancePropertiesFile() throws Exception {
            // Given
            InstanceProperties properties = createTestInstanceProperties();
            helper.setEnvironmentVariable("EDITOR", "myeditor");

            // When / Then
            assertThat(helper.openInstancePropertiesGetCommandRun(properties))
                    .containsExactly("myeditor", tempDir.resolve("sleeper/admin/temp.properties").toString());
        }

        @Test
        void shouldWriteInstancePropertiesFile() throws Exception {
            // Given
            InstanceProperties properties = createTestInstanceProperties();

            // When / Then
            assertThat(helper.openInstancePropertiesGetPropertiesWritten(properties))
                    .isEqualTo(properties);
        }

        @Test
        void shouldGetDiffAfterPropertiesChanged() throws Exception {
            // Given
            InstanceProperties before = createTestInstancePropertiesWithId(id);
            before.set(INGEST_SOURCE_BUCKET, "bucket-before");
            InstanceProperties after = createTestInstancePropertiesWithId(id);
            after.set(INGEST_SOURCE_BUCKET, "bucket-after");

            // When / Then
            assertThat(helper.updateProperties(before, after).getDiff())
                    .extracting(PropertiesDiff::getChanges).asInstanceOf(LIST)
                    .containsExactly(valueChanged(INGEST_SOURCE_BUCKET, "bucket-before", "bucket-after"));
        }

        @Test
        void shouldRetrievePropertiesAfterChange() throws Exception {
            // Given
            InstanceProperties before = createTestInstanceProperties();
            InstanceProperties after = createTestInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");

            // When
            InstanceProperties properties = helper.updateProperties(before, after).getUpdatedProperties();

            // Then
            assertThat(properties).isEqualTo(after);
        }

        @Test
        void shouldFormatPropertiesUsingPrettyPrinter() throws Exception {
            // Given
            InstanceProperties properties = createTestInstanceProperties();

            // When
            String tempFileString = Files.readString(helper.openInstancePropertiesGetPathToFile(properties));

            // Then
            StringWriter writer = new StringWriter();
            properties.saveUsingPrettyPrinter(new PrintWriter(writer));
            assertThat(tempFileString).isEqualTo(writer.toString());
        }
    }

    @Nested
    @DisplayName("Open table properties file")
    class OpenTableProperties {

        @Test
        void shouldUpdateTableProperties() throws Exception {
            // Given
            TableProperties before = generateCompareTestTableProperties();
            before.set(ROW_GROUP_SIZE, "123");
            TableProperties after = generateCompareTestTableProperties();
            after.set(ROW_GROUP_SIZE, "456");

            // When
            PropertiesDiff diff = helper.updateProperties(before, after).getDiff();

            // Then
            assertThat(diff)
                    .extracting(PropertiesDiff::getChanges).asInstanceOf(LIST)
                    .containsExactly(valueChanged(ROW_GROUP_SIZE, "123", "456"));
        }

        @Test
        void shouldRetrieveTablePropertiesAfterChange() throws Exception {
            // Given
            TableProperties before = generateCompareTestTableProperties();
            TableProperties after = generateCompareTestTableProperties();
            after.set(ROW_GROUP_SIZE, "456");

            // When
            TableProperties properties = helper.updateProperties(before, after).getUpdatedProperties();

            // Then
            assertThat(properties).isEqualTo(after);
        }
    }

    @Nested
    @DisplayName("Filter by property group")
    class FilterByGroup {

        @Test
        void shouldWriteSingleInstancePropertyGroupToFile() throws Exception {
            // Given
            InstanceProperties properties = createTestInstanceProperties();
            properties.set(LOGGING_LEVEL, "ERROR");

            // When / Then
            assertThat(helper.openFileGetPropertiesWritten(updater -> updater.openPropertiesFile(properties, InstancePropertyGroup.LOGGING)))
                    .isEqualTo(loadProperties("" +
                            "sleeper.logging.level=ERROR"));
        }

        @Test
        void shouldFormatPropertiesUsingPrettyPrinter() throws Exception {
            // Given
            InstanceProperties properties = createTestInstanceProperties();
            properties.set(LOGGING_LEVEL, "ERROR");

            // When
            String tempFileString = Files.readString(helper.openFileGetPathToFile(updater -> updater.openPropertiesFile(properties, InstancePropertyGroup.LOGGING)));

            // Then
            StringWriter writer = new StringWriter();
            InstanceProperties.createPrettyPrinterWithGroup(
                    new PrintWriter(writer), InstancePropertyGroup.LOGGING)
                    .print(properties);
            assertThat(tempFileString).isEqualTo(writer.toString());
        }

        @Test
        void shouldCreateUpdateRequestWithInstanceProperties() throws Exception {
            // Given
            InstanceProperties before = createTestInstancePropertiesWithId(id);
            before.set(LOGGING_LEVEL, "ERROR");
            InstanceProperties after = createTestInstancePropertiesWithId(id);
            after.set(LOGGING_LEVEL, "INFO");

            // When
            UpdatePropertiesRequest<InstanceProperties> updatePropertiesRequest = helper.updatePropertiesWithGroup(
                    before, "sleeper.logging.level=INFO", InstancePropertyGroup.LOGGING);

            // Then
            assertThat(updatePropertiesRequest.getUpdatedProperties())
                    .isEqualTo(after);
            assertThat(updatePropertiesRequest.getDiff())
                    .isEqualTo(new PropertiesDiff(before, after));
        }

        @Test
        void shouldCreateUpdateRequestWithTableProperties() throws Exception {
            // Given
            TableProperties before = generateCompareTestTableProperties();
            before.set(STATESTORE_ASYNC_COMMITS_ENABLED, "false");
            TableProperties after = generateCompareTestTableProperties();
            after.set(STATESTORE_ASYNC_COMMITS_ENABLED, "true");

            // When
            UpdatePropertiesRequest<TableProperties> updatePropertiesRequest = helper.updatePropertiesWithGroup(
                    before, "sleeper.table.statestore.commit.async.enabled=true", TablePropertyGroup.METADATA);

            // Then
            assertThat(updatePropertiesRequest.getUpdatedProperties())
                    .isEqualTo(after);
            assertThat(updatePropertiesRequest.getDiff())
                    .isEqualTo(new PropertiesDiff(before, after));
        }

        @Test
        void shouldUnsetPropertyWhenRemovedInEditor() throws Exception {
            // Given
            InstanceProperties before = createTestInstancePropertiesWithId(id);
            before.set(LOGGING_LEVEL, "ERROR");
            InstanceProperties after = createTestInstancePropertiesWithId(id);
            after.unset(LOGGING_LEVEL);

            // When
            UpdatePropertiesRequest<InstanceProperties> updatePropertiesRequest = helper.updatePropertiesWithGroup(
                    before, "", InstancePropertyGroup.LOGGING);

            // Then
            assertThat(updatePropertiesRequest.getUpdatedProperties())
                    .isEqualTo(after);
            assertThat(updatePropertiesRequest.getDiff())
                    .isEqualTo(new PropertiesDiff(before, after));
        }

        @Test
        void shouldNotShowUnknownProperties() throws Exception {
            // Given
            InstanceProperties properties = createTestInstanceProperties();
            properties.getProperties().setProperty("unknown.property", "some-value");

            // When
            assertThat(helper.openFileGetPropertiesWritten(updater -> updater.openPropertiesFile(properties, InstancePropertyGroup.LOGGING)))
                    .isEmpty();
        }

        @Test
        void shouldUpdateAnUnknownProperty() throws Exception {
            // Given
            InstanceProperties before = createTestInstancePropertiesWithId(id);
            before.getProperties().setProperty("unknown.property", "value-before");
            InstanceProperties after = createTestInstancePropertiesWithId(id);
            after.getProperties().setProperty("unknown.property", "value-after");

            // When
            UpdatePropertiesRequest<InstanceProperties> updatePropertiesRequest = helper.updatePropertiesWithGroup(
                    before, "unknown.property=value-after", InstancePropertyGroup.LOGGING);

            // Then
            assertThat(updatePropertiesRequest.getUpdatedProperties())
                    .isEqualTo(after);
            assertThat(updatePropertiesRequest.getDiff())
                    .isEqualTo(new PropertiesDiff(before, after));
        }

        @Test
        void shouldUpdateAPropertyOutsideTheSpecifiedGroup() throws Exception {
            // Given
            InstanceProperties before = createTestInstancePropertiesWithId(id);
            before.set(INGEST_SOURCE_BUCKET, "bucket-before");
            InstanceProperties after = createTestInstancePropertiesWithId(id);
            after.set(INGEST_SOURCE_BUCKET, "bucket-after");

            // When
            UpdatePropertiesRequest<InstanceProperties> updatePropertiesRequest = helper.updatePropertiesWithGroup(
                    before, "sleeper.ingest.source.bucket=bucket-after", InstancePropertyGroup.LOGGING);

            // Then
            assertThat(updatePropertiesRequest.getUpdatedProperties())
                    .isEqualTo(after);
            assertThat(updatePropertiesRequest.getDiff())
                    .isEqualTo(new PropertiesDiff(before, after));
        }

        @Test
        void shouldLeaveUnknownPropertyUnchangedWhenEditingAnotherProperty() throws Exception {
            // Given
            InstanceProperties before = createTestInstancePropertiesWithId(id);
            before.getProperties().setProperty("unknown.property", "test-value");
            InstanceProperties after = createTestInstancePropertiesWithId(id);
            after.getProperties().setProperty("unknown.property", "test-value");
            after.set(LOGGING_LEVEL, "TRACE");

            // When
            UpdatePropertiesRequest<InstanceProperties> updatePropertiesRequest = helper.updatePropertiesWithGroup(
                    before, "sleeper.logging.level=TRACE", InstancePropertyGroup.LOGGING);

            // Then
            assertThat(updatePropertiesRequest.getUpdatedProperties())
                    .isEqualTo(after);
            assertThat(updatePropertiesRequest.getDiff())
                    .isEqualTo(new PropertiesDiff(before, after));
        }
    }

    public static TableProperties generateCompareTestTableProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, "test-instance");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "test-table");
        tableProperties.setSchema(createSchemaWithKey("key"));
        return tableProperties;
    }
}
