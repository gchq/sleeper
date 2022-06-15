package sleeper.ingest.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchAcceptingRecords;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchAcceptingRecords;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class StandardIngestCoordinator {
    private StandardIngestCoordinator() {
    }

    public static IngestCoordinator<Record> directWriteBackedByArrayList(ObjectFactory objectFactory,
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
                                                                         String filePathPrefix,
                                                                         int maxNoOfRecordsInMemory,
                                                                         long maxNoOfRecordsInLocalStore)
            throws StateStoreException {
        Supplier<RecordBatch<Record>> recordBatchFactoryFn = () -> new ArrayListRecordBatchAcceptingRecords(
                sleeperSchema,
                localWorkingDirectory,
                maxNoOfRecordsInMemory,
                maxNoOfRecordsInLocalStore,
                parquetRowGroupSize,
                parquetPageSize,
                parquetCompressionCodec,
                hadoopConfiguration);
        Function<Partition, PartitionFileWriter> partitionFileFactoryFn = partition -> {
            try {
                return new DirectPartitionFileWriter(
                        sleeperSchema,
                        partition,
                        parquetRowGroupSize,
                        parquetPageSize,
                        parquetCompressionCodec,
                        hadoopConfiguration,
                        filePathPrefix);
            } catch (Exception e) {
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

    public static IngestCoordinator<Record> directWriteBackedByArrow(ObjectFactory objectFactory,
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
                                                                     String filePathPrefix,
                                                                     BufferAllocator arrowBufferAllocator,
                                                                     int maxNoOfRecordsToWriteToArrowFileAtOnce,
                                                                     long workingArrowBufferAllocatorBytes,
                                                                     long minBatchArrowBufferAllocatorBytes,
                                                                     long maxBatchArrowBufferAllocatorBytes,
                                                                     long maxNoOfBytesToWriteLocally) throws StateStoreException {
        Supplier<RecordBatch<Record>> recordBatchFactoryFn = () ->
                new ArrowRecordBatchAcceptingRecords(
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
                return new DirectPartitionFileWriter(
                        sleeperSchema,
                        partition,
                        parquetRowGroupSize,
                        parquetPageSize,
                        parquetCompressionCodec,
                        hadoopConfiguration,
                        filePathPrefix);
            } catch (Exception e) {
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

    public static IngestCoordinator<Record> asyncS3WriteBackedByArrow(ObjectFactory objectFactory,
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
                                                                      long maxNoOfBytesToWriteLocally) throws StateStoreException {
        Supplier<RecordBatch<Record>> recordBatchFactoryFn = () ->
                new ArrowRecordBatchAcceptingRecords(
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
