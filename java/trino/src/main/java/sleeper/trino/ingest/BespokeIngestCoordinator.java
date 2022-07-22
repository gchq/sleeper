package sleeper.trino.ingest;

import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.statestore.StateStore;
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
                                                        String localWorkingDirectory,
                                                        int parquetRowGroupSize,
                                                        int parquetPageSize,
                                                        String parquetCompressionCodec,
                                                        Configuration hadoopConfiguration,
                                                        String sleeperIteratorClassName,
                                                        String sleeperIteratorConfig,
                                                        int ingestPartitionRefreshFrequencyInSeconds,
                                                        String s3BucketName,
                                                        S3AsyncClient s3AsyncClient,
                                                        BufferAllocator arrowBufferAllocator,
                                                        int maxNoOfRecordsToWriteToArrowFileAtOnce,
                                                        long workingArrowBufferAllocatorBytes,
                                                        long minBatchArrowBufferAllocatorBytes,
                                                        long maxBatchArrowBufferAllocatorBytes,
                                                        long maxNoOfBytesToWriteLocally) {
        Supplier<RecordBatch<Page>> recordBatchFactoryFn = () ->
                new ArrowRecordBatchAcceptingPages(
                        arrowBufferAllocator,
                        sleeperSchema,
                        localWorkingDirectory,
                        workingArrowBufferAllocatorBytes,
                        minBatchArrowBufferAllocatorBytes,
                        maxBatchArrowBufferAllocatorBytes,
                        maxNoOfBytesToWriteLocally,
                        maxNoOfRecordsToWriteToArrowFileAtOnce);
        Function<Partition, PartitionFileWriter> partitionFileFactoryFn = partition -> {
            try {
                return new AsyncS3PartitionFileWriter(
                        sleeperSchema,
                        partition,
                        parquetRowGroupSize,
                        parquetPageSize,
                        parquetCompressionCodec,
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
