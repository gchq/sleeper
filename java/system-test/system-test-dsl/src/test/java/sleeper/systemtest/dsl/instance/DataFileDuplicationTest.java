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
package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryDataFilesDriver;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithFormat;

public class DataFileDuplicationTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
    private final InMemoryRowStore data = new InMemoryRowStore();
    PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
    Supplier<String> filenameSupplier = supplyNumberedIdsWithFormat("duplicate-%s.parquet");

    @Test
    void shouldDuplicateFiles() {
        // Given
        addFile("file-1.parquet", List.of(new Row(Map.of("key", "value-1"))));
        addFile("file-2.parquet", List.of(new Row(Map.of("key", "value-2")), new Row(Map.of("key", "value-3"))));
        List<FileReference> originalReferences = List.of(
                referenceFactory().rootFile("file-1.parquet", 1),
                referenceFactory().rootFile("file-2.parquet", 2));

        // When
        List<FileReference> results = DataFileDuplication.duplicateByReferences(driver(), 1, originalReferences);

        // Then
        assertThat(results).containsExactly(
                referenceFactory().rootFile("duplicate-1.parquet", 1),
                referenceFactory().rootFile("duplicate-2.parquet", 2));
        assertThat(readRows("duplicate-1.parquet"))
                .containsExactly(new Row(Map.of("key", "value-1")));
        assertThat(readRows("duplicate-1.parquet"))
                .containsExactly(new Row(Map.of("key", "value-2")), new Row(Map.of("key", "value-3")));
    }

    @Test
    void shouldDuplicateFilesWithDifferentReferences() {
        // Given
        partitions = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "m")
                .buildTree();
        addFile("file-1.parquet", List.of(new Row(Map.of("key", "apple"))));
        addFile("file-2.parquet", List.of(new Row(Map.of("key", "barbecue")), new Row(Map.of("key", "portrait"))));
        FileReference leftFile = referenceFactory().partitionFile("L", "file-1.parquet", 1);
        FileReference splitFile = referenceFactory().rootFile("file-2.parquet", 2);
        List<FileReference> originalReferences = List.of(leftFile,
                SplitFileReference.referenceForChildPartition(splitFile, "L", 1),
                SplitFileReference.referenceForChildPartition(splitFile, "R", 1));

        // When
        List<FileReference> results = DataFileDuplication.duplicateByReferences(driver(), 1, originalReferences);

        // Then
        FileReference newLeftFile = referenceFactory().partitionFile("L", "duplicate-1.parquet", 1);
        FileReference newSpanningFile = referenceFactory().rootFile("duplicate-2.parquet", 2);
        assertThat(results).containsExactly(newLeftFile,
                SplitFileReference.referenceForChildPartition(newSpanningFile, "L", 1),
                SplitFileReference.referenceForChildPartition(newSpanningFile, "R", 1));
        assertThat(readRows("duplicate-1.parquet"))
                .containsExactly(new Row(Map.of("key", "apple")));
        assertThat(readRows("duplicate-2.parquet"))
                .containsExactly(new Row(Map.of("key", "barbecue")), new Row(Map.of("key", "portrait")));
    }

    private void addFile(String filename, List<Row> rows) {
        data.addFile(filename, rows);
    }

    private List<Row> readRows(String filename) {
        return data.streamRows(List.of(filename)).toList();
    }

    private FileReferenceFactory referenceFactory() {
        return FileReferenceFactory.from(partitions);
    }

    private DataFilesDriver driver() {
        return new InMemoryDataFilesDriver(data, filenameSupplier);
    }

}
