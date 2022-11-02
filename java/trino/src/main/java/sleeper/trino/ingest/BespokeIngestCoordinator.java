/*
 * Copyright 2022 Crown Copyright
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
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.statestore.StateStore;
import sleeper.trino.SleeperConfig;
import sleeper.trino.remotesleeperconnection.SleeperRawAwsConnection;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class BespokeIngestCoordinator {

    private BespokeIngestCoordinator() {
    }

    public static IngestCoordinator<Page> asyncFromPage(ObjectFactory objectFactory,
                                                        StateStore sleeperStateStore,
                                                        Schema sleeperSchema,
                                                        SleeperConfig sleeperConfig,
                                                        Configuration hadoopConfiguration,
                                                        String sleeperIteratorClassName,
                                                        String sleeperIteratorConfig,
                                                        int ingestPartitionRefreshFrequencyInSeconds,
                                                        String s3BucketName,
                                                        S3AsyncClient s3AsyncClient,
                                                        BufferAllocator arrowBufferAllocator) {
        String localWorkingDirectory = sleeperConfig.getLocalWorkingDirectory();
        long maxBytesToWriteLocally = sleeperConfig.getMaxBytesToWriteLocallyPerWriter();
        long maxBatchArrowBufferAllocatorBytes = sleeperConfig.getMaxArrowRootAllocatorBytes();

        Supplier<RecordBatch<Page>> recordBatchFactoryFn = () ->
                new ArrowRecordBatchAcceptingPages(
                        arrowBufferAllocator,
                        sleeperSchema,
                        localWorkingDirectory,
                        SleeperRawAwsConnection.WORKING_ARROW_BUFFER_ALLOCATOR_BYTES,
                        SleeperRawAwsConnection.BATCH_ARROW_BUFFER_ALLOCATOR_BYTES_MIN,
                        maxBatchArrowBufferAllocatorBytes,
                        maxBytesToWriteLocally,
                        SleeperRawAwsConnection.MAX_NO_OF_RECORDS_TO_WRITE_TO_ARROW_FILE_AT_ONCE);
        Function<Partition, PartitionFileWriter> partitionFileFactoryFn = partition -> {
            try {
                return new AsyncS3PartitionFileWriter(
                        sleeperSchema,
                        partition,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        SleeperRawAwsConnection.INGEST_COMPRESSION_CODEC,
                        hadoopConfiguration,
                        s3BucketName,
                        s3AsyncClient,
                        localWorkingDirectory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return new IngestCoordinator<>(
                objectFactory,
                sleeperStateStore,
                sleeperSchema,
                sleeperIteratorClassName,
                sleeperIteratorConfig,
                ingestPartitionRefreshFrequencyInSeconds,
                recordBatchFactoryFn,
                partitionFileFactoryFn);
    }
}
