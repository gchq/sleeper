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
package sleeper.systemtest.drivers.util;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.table.TableFilePaths;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.systemtest.drivers.instance.AwsDataFilesDriver;
import sleeper.systemtest.dsl.instance.DataFilesDriver;
import sleeper.systemtest.dsl.util.DataFileDuplications;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DataFileDuplicationIT extends LocalStackTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
    PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Test
    void shouldDuplicateFiles() {
        // Given
        FileReference file1 = referenceFactory().rootFile("file-1", 1);
        FileReference file2 = referenceFactory().rootFile("file-2", 1);
        writeRows(file1, List.of(new Row(Map.of("key", "value-1"))));
        writeRows(file2, List.of(new Row(Map.of("key", "value-2")), new Row(Map.of("key", "value-3"))));

        // When
        List<FileReference> results = duplicateByReferences(1, List.of(file1, file2));

        // Then
        assertThat(results).hasSize(2);
        assertThat(readRows(results.get(0)))
                .containsExactly(new Row(Map.of("key", "value-1")));
        assertThat(readRows(results.get(1)))
                .containsExactly(new Row(Map.of("key", "value-2")), new Row(Map.of("key", "value-3")));
    }

    private List<FileReference> duplicateByReferences(int duplicates, List<FileReference> references) {
        return DataFileDuplications.duplicateByReferences(driver(), duplicates, references).streamNewReferences().toList();
    }

    private FileReferenceFactory referenceFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, partitions);
    }

    private void writeRows(FileReference file, List<Row> rows) {
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                new Path(file.getFilename()), tableProperties.getSchema(), hadoopConf)) {
            for (Row row : rows) {
                writer.write(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<Row> readRows(FileReference file) {
        try (ParquetReaderIterator iterator = new ParquetReaderIterator(
                ParquetRowReaderFactory.parquetRowReaderBuilder(
                        new Path(file.getFilename()), tableProperties.getSchema())
                        .withConf(hadoopConf)
                        .build())) {
            List<Row> rows = new ArrayList<>();
            iterator.forEachRemaining(rows::add);
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DataFilesDriver driver() {
        return new AwsDataFilesDriver(s3AsyncClient, hadoopConf, TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties));
    }

}
