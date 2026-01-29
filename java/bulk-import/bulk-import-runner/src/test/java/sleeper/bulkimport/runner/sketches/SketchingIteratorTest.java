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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.bulkimport.runner.common.SparkSketchRow;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SketchingIteratorTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new IntType()));
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());
    private final SketchesStore sketchesStore = new LocalFileSystemSketchesStore();
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(BULK_IMPORT_BUCKET, tempDir.toString());
    }

    @Test
    void shouldBuildOneSketchWithOneRow() {
        // Given
        List<Row> input = List.of(
                new Row(Map.of("key", 123)));

        // When / Then
        assertThat(applySketchingIterator(input))
                .containsExactly(new Result("root", SketchesDeciles.from(tableProperties, input)));
    }

    private List<Result> applySketchingIterator(List<Row> input) {
        SketchWritingIterator iterator = new SketchWritingIterator(
                toSparkRowIterator(input), instanceProperties, tableProperties, new Configuration(), new PartitionTree(stateStore.getAllPartitions()));
        List<Result> results = new ArrayList<>();
        while (iterator.hasNext()) {
            SparkSketchRow sketchRow = SparkSketchRow.from(iterator.next());
            Sketches sketches = sketchesStore.loadFileSketches(sketchRow.filename(), tableProperties.getSchema());
            results.add(new Result(sketchRow.partitionId(), SketchesDeciles.from(sketches)));
        }
        return results;
    }

    private Iterator<org.apache.spark.sql.Row> toSparkRowIterator(List<Row> rows) {
        return rows.stream().map(this::toSparkRow).iterator();
    }

    private org.apache.spark.sql.Row toSparkRow(Row row) {
        List<Object> values = new ArrayList<>();
        for (Field field : tableProperties.getSchema().getAllFields()) {
            values.add(row.get(field.getName()));
        }
        return RowFactory.create(values.toArray());
    }

    public record Result(String partitionId, SketchesDeciles sketches) {
    }

}
