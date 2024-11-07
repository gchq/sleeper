/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.trino.ingest;

import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.ingest.runner.impl.partitionfilewriter.AsyncS3PartitionFileWriterFactory;
import sleeper.ingest.runner.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.runner.impl.recordbatch.RecordBatchFactory;
import sleeper.ingest.runner.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.trino.SleeperConfig;
import sleeper.trino.remotesleeperconnection.SleeperRawAwsConnection;

public class BespokeIngestCoordinator {

    private BespokeIngestCoordinator() {
    }

    public static IngestCoordinator<Page> asyncFromPage(
            ObjectFactory objectFactory,
            StateStore sleeperStateStore,
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            SleeperConfig sleeperConfig,
            Configuration hadoopConfiguration,
            String sleeperIteratorClassName,
            String sleeperIteratorConfig,
            int ingestPartitionRefreshFrequencyInSeconds,
            S3AsyncClient s3AsyncClient,
            BufferAllocator arrowBufferAllocator) {
        String localWorkingDirectory = sleeperConfig.getLocalWorkingDirectory();
        long maxBytesToWriteLocally = sleeperConfig.getMaxBytesToWriteLocallyPerWriter();
        long maxBatchArrowBufferAllocatorBytes = sleeperConfig.getMaxArrowRootAllocatorBytes();

        RecordBatchFactory<Page> recordBatchFactory = ArrowRecordBatchFactory.builder()
                .bufferAllocator(arrowBufferAllocator)
                .schema(tableProperties.getSchema())
                .localWorkingDirectory(localWorkingDirectory)
                .workingBufferAllocatorBytes(SleeperRawAwsConnection.WORKING_ARROW_BUFFER_ALLOCATOR_BYTES)
                .minBatchBufferAllocatorBytes(SleeperRawAwsConnection.BATCH_ARROW_BUFFER_ALLOCATOR_BYTES_MIN)
                .maxBatchBufferAllocatorBytes(maxBatchArrowBufferAllocatorBytes)
                .maxNoOfBytesToWriteLocally(maxBytesToWriteLocally)
                .maxNoOfRecordsToWriteToArrowFileAtOnce(SleeperRawAwsConnection.MAX_NO_OF_RECORDS_TO_WRITE_TO_ARROW_FILE_AT_ONCE)
                .recordWriter(new ArrowRecordWriterAcceptingPages())
                .build();
        ParquetConfiguration parquetConfiguration = ParquetConfiguration.builder()
                .tableProperties(tableProperties)
                .hadoopConfiguration(hadoopConfiguration)
                .build();
        PartitionFileWriterFactory partitionFileWriterFactory = AsyncS3PartitionFileWriterFactory
                .builderWith(instanceProperties, tableProperties)
                .parquetConfiguration(parquetConfiguration)
                .s3AsyncClient(s3AsyncClient)
                .localWorkingDirectory(localWorkingDirectory)
                .build();
        return IngestCoordinator.builder()
                .objectFactory(objectFactory)
                .stateStore(sleeperStateStore)
                .schema(tableProperties.getSchema())
                .iteratorClassName(sleeperIteratorClassName)
                .iteratorConfig(sleeperIteratorConfig)
                .ingestPartitionRefreshFrequencyInSeconds(ingestPartitionRefreshFrequencyInSeconds)
                .recordBatchFactory(recordBatchFactory)
                .partitionFileWriterFactory(partitionFileWriterFactory)
                .build();
    }
}
