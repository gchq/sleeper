/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.type.StringType;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.ReadSplitPoints.readSplitPoints;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_BASE64_ENCODED;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class ReadSplitPointsIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @TempDir
    private Path tempDir;

    @Test
    void shouldReadSplitPointsFile() throws Exception {
        // Given
        Path splitPointsFile = tempDir.resolve("splits.txt");
        Files.writeString(splitPointsFile, "1\n2\n3");
        tableProperties.setSchema(schemaWithKey("key", new StringType()));
        tableProperties.set(SPLIT_POINTS_FILE, splitPointsFile.toString());

        // When / Then
        assertThat(readSplitPoints(tableProperties))
                .containsExactly("1", "2", "3");
    }

    @Test
    void shouldReadSplitPointsFileWithBase64EncodedStrings() throws Exception {
        // Given
        Path splitPointsFile = tempDir.resolve("splits.txt");
        Files.writeString(splitPointsFile, "MQ==\nMg==\nMw==");
        tableProperties.setSchema(schemaWithKey("key", new StringType()));
        tableProperties.set(SPLIT_POINTS_FILE, splitPointsFile.toString());
        tableProperties.set(SPLIT_POINTS_BASE64_ENCODED, "true");

        // When / Then
        assertThat(readSplitPoints(tableProperties))
                .containsExactly("1", "2", "3");
    }
}
