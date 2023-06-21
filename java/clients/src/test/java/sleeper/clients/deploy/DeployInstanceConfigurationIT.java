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

package sleeper.clients.deploy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.SchemaSerDe;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DeployInstanceConfigurationIT {
    @TempDir
    private Path tempDir;

    @Nested
    @DisplayName("Load from template directory")
    class LoadFromTemplateDirectory {
        @Test
        void shouldLoadInstanceAndTableProperties() throws Exception {
            // Given
            Files.writeString(tempDir.resolve("instanceproperties.template"), "sleeper.id=template-instance");
            Files.writeString(tempDir.resolve("tableproperties.template"), "sleeper.table.name=template-table");
            Files.writeString(tempDir.resolve("schema.template"), new SchemaSerDe().toJson(schemaWithKey("key")));

            // When
            DeployInstanceConfiguration instanceConfiguration = DeployInstanceConfiguration.fromTemplateDirectory(tempDir);

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(ID, "template-instance");
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "template-table");
            expectedTableProperties.setSchema(schemaWithKey("key"));
            assertThat(instanceConfiguration)
                    .isEqualTo(DeployInstanceConfiguration.builder()
                            .instanceProperties(expectedInstanceProperties)
                            .tableProperties(expectedTableProperties)
                            .build());
        }
    }
}
