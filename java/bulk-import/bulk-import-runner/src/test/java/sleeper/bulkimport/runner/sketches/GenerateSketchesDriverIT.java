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
package sleeper.bulkimport.runner.sketches;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.bulkimport.runner.SparkTestBase;
import sleeper.core.row.Row;
import sleeper.core.schema.type.IntType;
import sleeper.sketches.Sketches;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class GenerateSketchesDriverIT extends SparkTestBase {

    @BeforeEach
    void setUp() {
        createTable(createSchemaWithKey("key", new IntType()));
        update(stateStore()).initialise(partitionsBuilder()
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50)
                .buildList());
    }

    @Test
    void shouldGenerateSketches() {
        // Given
        String filename = writeRowsToFile(List.of(
                new Row(Map.of("key", 20)),
                new Row(Map.of("key", 70))));

        // When
        Map<String, Sketches> partitionIdToSketches = generateSketches(List.of(filename));

        // Then
        assertThat(toDeciles(partitionIdToSketches))
                .isEqualTo(Map.of(
                        "L", expectedSketchDeciles(List.of(
                                new Row(Map.of("key", 20)))),
                        "R", expectedSketchDeciles(List.of(
                                new Row(Map.of("key", 70))))));
    }

    private Map<String, SketchesDeciles> toDeciles(Map<String, Sketches> partitionIdToSketches) {
        return partitionIdToSketches.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), SketchesDeciles.from(entry.getValue())))
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    private Map<String, Sketches> generateSketches(List<String> filenames) {
        try (BulkImportSparkContext context = createBulkImportContext(filenames)) {
            return GenerateSketchesDriver.generatePartitionIdToSketches(context);
        }
    }

}
