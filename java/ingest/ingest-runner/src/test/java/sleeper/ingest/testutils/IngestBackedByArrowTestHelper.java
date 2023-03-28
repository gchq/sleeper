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

import org.apache.hadoop.conf.Configuration;

import sleeper.core.iterator.IteratorException;
import sleeper.core.key.Key;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;

public class IngestBackedByArrowTestHelper {

    private IngestBackedByArrowTestHelper() {
    }

    public static void ingestAndVerifyUsingDirectWriteBackedByArrow(
            Path temporaryFolder, Configuration hadoopConfiguration,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            StateStore stateStore,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes) throws IOException, StateStoreException, IteratorException {

        ingestAndVerifyUsingDirectWriteBackedByArrow(
                temporaryFolder, hadoopConfiguration,
                recordListAndSchema, stateStore,
                keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap,
                arrowWorkingBytes, arrowBatchBytes, localStoreBytes,
                new ArrowRecordWriterAcceptingRecords(),
                recordListAndSchema.recordList);
    }

    public static <T> void ingestAndVerifyUsingDirectWriteBackedByArrow(
            Path temporaryFolder,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            StateStore stateStore,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes,
            ArrowRecordWriter<T> recordWriter,
            Iterable<T> toWrite) throws IOException, StateStoreException, IteratorException {

        ingestAndVerifyUsingDirectWriteBackedByArrow(
                temporaryFolder, new Configuration(), recordListAndSchema, stateStore,
                keyToPartitionNoMappingFn, partitionNoToExpectedNoOfFilesMap,
                arrowWorkingBytes, arrowBatchBytes, localStoreBytes,
                recordWriter, toWrite);
    }

    public static <T> void ingestAndVerifyUsingDirectWriteBackedByArrow(
            Path temporaryFolder, Configuration hadoopConfiguration,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            StateStore stateStore,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes,
            ArrowRecordWriter<T> recordWriter,
            Iterable<T> toWrite) throws IOException, StateStoreException, IteratorException {
        String localWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();

        try (IngestCoordinator<T> ingestCoordinator = buildIngestCoordinator(temporaryFolder,
                stateStore, recordListAndSchema.sleeperSchema, hadoopConfiguration,
                localWorkingDirectory, arrowWorkingBytes, arrowBatchBytes, localStoreBytes, recordWriter)) {
            for (T write : toWrite) {
                ingestCoordinator.write(write);
            }
        }

        ResultVerifier.verify(
                stateStore,
                recordListAndSchema.sleeperSchema,
                keyToPartitionNoMappingFn,
                recordListAndSchema.recordList,
                partitionNoToExpectedNoOfFilesMap,
                hadoopConfiguration,
                localWorkingDirectory);
    }

    public static <T> IngestCoordinator<T> buildIngestCoordinator(
            Path temporaryFolder,
            StateStore stateStore, Schema schema, Configuration hadoopConfiguration,
            String localWorkingDirectory,
            long arrowWorkingBytes,
            long arrowBatchBytes,
            long localStoreBytes,
            ArrowRecordWriter<T> recordWriter) throws IOException {
        String ingestToDirectory = createTempDirectory(temporaryFolder, null).toString();
        ParquetConfiguration parquetConfiguration = parquetConfiguration(schema, hadoopConfiguration);
        return standardIngestCoordinator(
                stateStore, schema,
                ArrowRecordBatchFactory.builder()
                        .schema(schema)
                        .maxNoOfRecordsToWriteToArrowFileAtOnce(128)
                        .workingBufferAllocatorBytes(arrowWorkingBytes)
                        .minBatchBufferAllocatorBytes(arrowBatchBytes)
                        .maxBatchBufferAllocatorBytes(arrowBatchBytes)
                        .maxNoOfBytesToWriteLocally(localStoreBytes)
                        .localWorkingDirectory(localWorkingDirectory)
                        .recordWriter(recordWriter)
                        .build(),
                DirectPartitionFileWriterFactory.from(
                        parquetConfiguration, ingestToDirectory));
    }
}
