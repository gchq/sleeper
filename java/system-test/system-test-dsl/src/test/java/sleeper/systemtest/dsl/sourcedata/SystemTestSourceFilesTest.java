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

package sleeper.systemtest.dsl.sourcedata;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class SystemTestSourceFilesTest {

    @Test
    void shouldGenerateFilenameForSourceFile(SleeperSystemTest sleeper, SystemTestContext context) {
        // Given
        Row row = new Row(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);

        // When
        sleeper.sourceFiles().create("test.parquet", row);

        // Then
        assertThat(context.sourceFiles().getFilePath("test.parquet"))
                .startsWith("file://in-memory-system-test-bucket/")
                .endsWith("/test.parquet");
    }
}
