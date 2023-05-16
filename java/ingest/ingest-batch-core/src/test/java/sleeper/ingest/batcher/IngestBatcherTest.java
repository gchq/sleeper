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

package sleeper.ingest.batcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class IngestBatcherTest {
    @Nested
    @DisplayName("Batch with minimum file count")
    class BatchWithMinimumFileCount {
        private final InstanceProperties instanceProperties = createTestInstanceProperties();
        private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        private final TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);

        @Test
        void shouldBatchOneFileWhenMinimumFileCountIsOne() {
            // Given
            IngestBatcher batcher = IngestBatcher.builder().tablePropertiesProvider(tablePropertiesProvider).build();
        }
    }
}
