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
package sleeper.ingest.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.conf.Configuration;

import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.statestore.StateStore;
import sleeper.statestore.inmemory.StateStoreTestBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithPartitions;

public class IngestTestHelper<T> {

    private final Path temporaryFolder;
    private final Configuration hadoopConfiguration;
    private final String localWorkingDirectory;
    private final Schema schema;
    private final List<Record> expectedRecords;
    private Iterable<T> toWrite;
    private StateStore stateStore;
    private RecordBatchFactory<T> recordBatchFactory;
    private PartitionFileWriterFactory partitionFileWriterFactory;

    private IngestTestHelper(Path temporaryFolder, Configuration hadoopConfiguration,
                             Schema schema, List<Record> expectedRecords, Iterable<T> toWrite) throws Exception {
        this.temporaryFolder = temporaryFolder;
        this.hadoopConfiguration = hadoopConfiguration;
        this.localWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        this.schema = schema;
        this.expectedRecords = expectedRecords;
        this.toWrite = toWrite;
    }

    public static IngestTestHelper<Record> from(
            Path temporaryFolder, Configuration hadoopConfiguration,
            RecordGenerator.RecordListAndSchema recordListAndSchema) throws Exception {
        return from(temporaryFolder, hadoopConfiguration,
                recordListAndSchema.sleeperSchema, recordListAndSchema.recordList);
    }

    public static IngestTestHelper<Record> from(
            Path temporaryFolder, Configuration hadoopConfiguration,
            Schema schema, List<Record> records) throws Exception {
        return new IngestTestHelper<>(temporaryFolder, hadoopConfiguration, schema, records, records);
    }

    public void ingestAndVerify(
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap) throws Exception {

        try (IngestCoordinator<T> ingestCoordinator = standardIngestCoordinator(
                stateStore, schema, recordBatchFactory, partitionFileWriterFactory)) {
            for (T write : toWrite) {
                ingestCoordinator.write(write);
            }
        }

        ResultVerifier.verify(
                stateStore,
                schema,
                keyToPartitionNoMappingFn,
                expectedRecords,
                partitionNoToExpectedNoOfFilesMap,
                hadoopConfiguration,
                localWorkingDirectory);
    }

    public IngestTestHelper<T> createStateStore(
            AmazonDynamoDB dynamoDbClient,
            PartitionTree tree) throws Exception {
        StateStore stateStore = inMemoryStateStoreWithPartitions(tree.getAllPartitions());
        return stateStore(stateStore);
    }

    public IngestTestHelper<T> stateStoreInMemory(
            Consumer<PartitionsBuilder> partitionsConfig) {
        return stateStoreInMemory(partitionsConfig, files -> {
        });
    }

    public IngestTestHelper<T> stateStoreInMemory(
            Consumer<PartitionsBuilder> partitionsConfig,
            Consumer<StateStoreTestBuilder> filesConfig) {
        PartitionsBuilder partitions = new PartitionsBuilder(schema);
        partitionsConfig.accept(partitions);
        StateStoreTestBuilder builder = StateStoreTestBuilder.from(partitions);
        filesConfig.accept(builder);
        return stateStore(builder.buildStateStore());
    }

    public IngestTestHelper<T> directWrite() throws IOException {
        ParquetConfiguration parquetConfiguration = parquetConfiguration(schema, hadoopConfiguration);
        String ingestToDirectory = createTempDirectory(temporaryFolder, null).toString();
        return partitionFileWriterFactory(
                DirectPartitionFileWriterFactory.from(parquetConfiguration, ingestToDirectory));
    }

    public IngestTestHelper<Record> backedByArrow(
            Consumer<ArrowRecordBatchFactory.Builder<Record>> configuration) {
        return backedByArrowWithRecordWriter(new ArrowRecordWriterAcceptingRecords(), configuration);
    }

    public <N> IngestTestHelper<N> backedByArrowWithRecordWriter(
            ArrowRecordWriter<N> recordWriter,
            Consumer<ArrowRecordBatchFactory.Builder<N>> configuration) {
        ArrowRecordBatchFactory.Builder<N> builder = ArrowRecordBatchFactory.builder().schema(schema)
                .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                .localWorkingDirectory(localWorkingDirectory)
                .recordWriter(recordWriter);
        configuration.accept(builder);
        return recordBatchFactory(builder.build());
    }

    public IngestTestHelper<T> stateStore(StateStore stateStore) {
        this.stateStore = stateStore;
        return this;
    }

    public <N> IngestTestHelper<N> toWrite(Iterable<N> toWrite) {
        this.toWrite = (Iterable<T>) toWrite;
        return (IngestTestHelper<N>) this;
    }

    public <N> IngestTestHelper<N> recordBatchFactory(RecordBatchFactory<N> recordBatchFactory) {
        this.recordBatchFactory = (RecordBatchFactory<T>) recordBatchFactory;
        return (IngestTestHelper<N>) this;
    }

    public IngestTestHelper<T> partitionFileWriterFactory(PartitionFileWriterFactory partitionFileWriterFactory) {
        this.partitionFileWriterFactory = partitionFileWriterFactory;
        return this;
    }
}
