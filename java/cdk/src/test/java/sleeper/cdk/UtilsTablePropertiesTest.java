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
package sleeper.cdk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;

class UtilsTablePropertiesTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    private Path tempDir;
    private Path instancePropertiesFile;

    @BeforeEach
    void setUp() throws IOException {
        instancePropertiesFile = tempDir.resolve("instance.properties");
        instanceProperties.save(instancePropertiesFile);
    }

    private Schema schemaWithKey(String keyName) {
        return Schema.builder().rowKeyFields(new Field(keyName, new StringType())).build();
    }

    @Test
    void shouldFindTablePropertiesFileNextToInstancePropertiesFile() throws IOException {
        TableProperties properties = createTestTableProperties(instanceProperties, schemaWithKey("test-key"));
        properties.save(tempDir.resolve("table.properties"));
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .containsExactly(properties);
    }

    @Test
    void shouldFindNoTablePropertiesFiles() throws IOException {
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .isEmpty();
    }

    @Test
    void shouldFindTablePropertiesFilesInTablesFolder() throws IOException {
        Files.createDirectory(tempDir.resolve("tables"));
        TableProperties properties1 = createTestTableProperties(instanceProperties, schemaWithKey("test-key1"));
        properties1.save(tempDir.resolve("tables/table1.properties"));
        TableProperties properties2 = createTestTableProperties(instanceProperties, schemaWithKey("test-key2"));
        properties2.save(tempDir.resolve("tables/table2.properties"));
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .containsExactly(properties1, properties2);
    }
}
