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

package sleeper.configuration.properties.local;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadInstanceProperties;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadTablesFromPath;
import static sleeper.configuration.properties.local.SaveLocalProperties.save;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;

public class SaveLocalPropertiesTest {
    @TempDir
    private Path tempDir;

    @Test
    void shouldSaveInstanceProperties() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties();

        // When
        save(tempDir, properties, Stream.empty());

        // Then
        assertThat(loadInstanceProperties(new InstanceProperties(), tempDir.resolve("instance.properties")))
                .isEqualTo(properties);
    }

    @Test
    void shouldSaveTableProperties() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        TableProperties tableProperties = createTestTableProperties(properties, schema);

        // When
        save(tempDir, properties, Stream.of(tableProperties));

        // Then
        assertThat(loadTablesFromPath(properties, tempDir.resolve("instance.properties")))
                .containsExactly(tableProperties);
    }
}
