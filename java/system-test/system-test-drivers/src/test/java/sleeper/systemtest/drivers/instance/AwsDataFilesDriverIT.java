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
package sleeper.systemtest.drivers.instance;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.localstack.test.SleeperLocalStackClients;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.util.DataFileDuplications;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@LocalStackDslTest
public class AwsDataFilesDriverIT {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(LOCALSTACK_MAIN);
    }

    @Test
    void shouldReadFile(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.tables().create("test", DEFAULT_SCHEMA);
        sleeper.sourceFiles().inDataBucket()
                .createWithNumberedRows("test.parquet", LongStream.of(1, 3, 2));
        sleeper.ingest().toStateStore().addFileOnEveryPartition("test.parquet", 3);

        // When / Then
        assertThat(sleeper.tableFiles().all().getFilesWithReferences())
                .first().satisfies(file -> {
                    try (CloseableIterator<Row> iterator = sleeper.getRows(file)) {
                        assertThat(iterator)
                                .toIterable()
                                .containsExactlyElementsOf(sleeper.generateNumberedRows(LongStream.of(1, 3, 2)));
                    }
                });
    }

    @Test
    void shouldDuplicateFiles(SleeperSystemTest sleeper, SystemTestContext context) {
        // Given
        sleeper.tables().create("test", createSchemaWithKey("key", new StringType()));
        FileReference file1 = referenceFactory(context).rootFile("file-1", 1);
        FileReference file2 = referenceFactory(context).rootFile("file-2", 1);
        writeRows(context, file1, List.of(new Row(Map.of("key", "value-1"))));
        writeRows(context, file2, List.of(new Row(Map.of("key", "value-2")), new Row(Map.of("key", "value-3"))));

        // When
        DataFileDuplications duplications = sleeper.ingest().toStateStore().duplicateFilesOnSamePartitions(1, List.of(file1, file2));

        // Then
        List<FileReference> results = duplications.streamNewReferences().toList();
        assertThat(results).hasSize(2);
        assertThat(readRows(context, results.get(0)))
                .containsExactly(new Row(Map.of("key", "value-1")));
        assertThat(readRows(context, results.get(1)))
                .containsExactly(new Row(Map.of("key", "value-2")), new Row(Map.of("key", "value-3")));
    }

    private FileReferenceFactory referenceFactory(SystemTestContext context) {
        InstanceProperties instanceProperties = context.instance().getInstanceProperties();
        TableProperties tableProperties = context.instance().getTableProperties();
        StateStore stateStore = context.instance().getStateStore();
        return FileReferenceFactory.from(instanceProperties, tableProperties, stateStore);
    }

    private void writeRows(SystemTestContext context, FileReference file, List<Row> rows) {
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                new Path(file.getFilename()), schema(context), SleeperLocalStackClients.HADOOP_CONF)) {
            for (Row row : rows) {
                writer.write(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<Row> readRows(SystemTestContext context, FileReference file) {
        try (ParquetReaderIterator iterator = new ParquetReaderIterator(
                ParquetRowReaderFactory.parquetRowReaderBuilder(
                        new Path(file.getFilename()), schema(context))
                        .withConf(SleeperLocalStackClients.HADOOP_CONF)
                        .build())) {
            List<Row> rows = new ArrayList<>();
            iterator.forEachRemaining(rows::add);
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Schema schema(SystemTestContext context) {
        return context.instance().getTableProperties().getSchema();
    }
}
