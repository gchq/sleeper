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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.localstack.test.SleeperLocalStackClients;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperDsl;
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
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceNoTables(LOCALSTACK_MAIN);
    }

    @Test
    void shouldReadFile(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.tables().create("test", DEFAULT_SCHEMA);
        sleeper.sourceFiles().inDataBucket()
                .createWithNumberedRows("test.parquet", LongStream.of(1, 3, 2));
        sleeper.ingest().toStateStore().addFileOnEveryPartition("test.parquet", 3);

        // When / Then
        assertThat(sleeper.tableFiles().all().getFilesWithReferences())
                .first().satisfies(file -> {
                    try (CloseableIterator<Row> iterator = sleeper.getRows(file)) {
                        assertThat(iterator).toIterable().containsExactlyElementsOf(
                                sleeper.generateNumberedRows().iterableOver(1, 3, 2));
                    }
                });
    }

    @Test
    void shouldDuplicateFiles(SleeperDsl sleeper, SystemTestContext context) {
        // Given
        sleeper.tables().create("test", createSchemaWithKey("key", new StringType()));
        sleeper.ingest().toStateStore()
                .addFileOnPartition("file-1.parquet", "root", new Row(Map.of("key", "value-1")))
                .addFileOnPartition("file-2.parquet", "root", new Row(Map.of("key", "value-2")));

        // When
        DataFileDuplications duplications = sleeper.ingest().toStateStore().duplicateFilesOnSamePartitions(1);

        // Then
        List<FileReference> results = duplications.streamNewReferences().toList();
        assertThat(results).hasSize(2);
        assertThat(readRows(context, results.get(0)))
                .containsExactly(new Row(Map.of("key", "value-1")));
        assertThat(readRows(context, results.get(1)))
                .containsExactly(new Row(Map.of("key", "value-2")));
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
