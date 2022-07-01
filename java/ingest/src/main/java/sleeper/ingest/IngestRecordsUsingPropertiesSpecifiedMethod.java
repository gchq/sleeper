package sleeper.ingest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.partitionfilewriter.AsyncS3PartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriter;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriter;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchAcceptingRecords;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchAcceptingRecords;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;
import static sleeper.configuration.properties.table.TableProperty.*;

/**
 * This class provide methods to support ingest into Sleeper, where the way in which that ingest should take place is
 * specified in the instance properties and the table properties.
 */
public class IngestRecordsUsingPropertiesSpecifiedMethod {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRecordsUsingPropertiesSpecifiedMethod.class);

    /**
     * Ingest all of the {@link Record} objects supplied by an iterator. The ingest mechanism is configured using
     * Sleeper {@link InstanceProperties} and {@link TableProperties}.
     * <p>
     * Note that the ingest process may take place asynchronously, but this method will only return once all of the
     * asynchronous ingests have completed.
     *
     * @param objectFactory            The object factory to use to create Sleeper iterators
     * @param sleeperStateStore        The state store to update with the new data
     * @param instanceProperties       The instance properties to use to configure the ingest
     * @param tableProperties          The table properties to use to configure the ingest
     * @param localWorkingDirectory    A local directory for temporary files
     * @param bufferAllocator          A buffer allocator to use during Arrow-based ingest. It may be null, and if it is
     *                                 needed, then a new {@link RootAllocator} will be created for this ingest and then
     *                                 closed
     * @param s3AsyncClient            A client to use during asynchronous ingest. It may be null, and if it is needed,
     *                                 a default will be created using {@link S3AsyncClient#create()} for this ingest
     *                                 and then closed
     * @param hadoopConfiguration      An Hadoop configuration to use when writing Parquet files. It may be null, and if
     *                                 it is, a default configuration will be used
     * @param sleeperIteratorClassName The name of the Sleeper iterator to apply
     * @param sleeperIteratorConfig    The configuration of the iterator
     * @param recordIterator           The iterator that provides the records to be ingested
     * @return A list of information about each new partition file that has been created in Sleeper
     * @throws StateStoreException -
     * @throws IteratorException   -
     * @throws IOException         -
     */
    public static List<FileInfo> ingestFromRecordIterator(
            ObjectFactory objectFactory,
            StateStore sleeperStateStore,
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            String localWorkingDirectory,
            BufferAllocator bufferAllocator,
            S3AsyncClient s3AsyncClient,
            Configuration hadoopConfiguration,
            String sleeperIteratorClassName,
            String sleeperIteratorConfig,
            CloseableIterator<Record> recordIterator) throws StateStoreException, IteratorException, IOException {
        // If the partition file writer is 'async' then create the default async S3 client if required
        S3AsyncClient internalS3AsyncClient =
                instanceProperties.get(INGEST_PARTITION_FILE_WRITER_TYPE).toLowerCase(Locale.ROOT).equals("async") ?
                        ((s3AsyncClient == null) ? S3AsyncClient.create() : s3AsyncClient) :
                        null;
        // If the Hadoop configuration is null then create a default configuration
        Configuration internalHadoopConfiguration = (hadoopConfiguration == null) ?
                defaultHadoopConfiguration() : hadoopConfiguration;
        // If the record batch type is Arrow, and no buffer allocator is provided, then create a root allocator that is
        // large enough to hold both the working and batch buffers.
        // This approach does not allow the batch buffer to be shared between multiple writing threads.
        long totalArrowBytesRequired = 0;
        if (instanceProperties.get(INGEST_RECORD_BATCH_TYPE).toLowerCase(Locale.ROOT).equals("arrow")) {
            totalArrowBytesRequired = instanceProperties.getLong(ARROW_INGEST_WORKING_BUFFER_BYTES) +
                    instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES);
        }
        try (BufferAllocator arrowBufferAllocator =
                     (totalArrowBytesRequired > 0) ?
                             ((bufferAllocator == null) ?
                                     new RootAllocator(totalArrowBytesRequired) :
                                     bufferAllocator.newChildAllocator("Ingest buffer", totalArrowBytesRequired, totalArrowBytesRequired)) :
                             null;
             // Create an IngestRecords object
             IngestCoordinator<Record> ingestCoordinator = createIngestCoordinatorFromProperties(
                     objectFactory,
                     sleeperStateStore,
                     instanceProperties,
                     tableProperties,
                     localWorkingDirectory,
                     arrowBufferAllocator,
                     internalS3AsyncClient,
                     internalHadoopConfiguration,
                     sleeperIteratorClassName,
                     sleeperIteratorConfig)) {
            // Write all the records to the IngestCoordinator
            while (recordIterator.hasNext()) {
                ingestCoordinator.write(recordIterator.next());
            }
            return ingestCoordinator.closeReturningFileInfoList();
            // The Arrow buffer will be auto-closed
        } finally {
            recordIterator.close();
            // Close the S3 client if it was created in this method
            if (s3AsyncClient == null && internalS3AsyncClient != null) {
                internalS3AsyncClient.close();
            }
        }
    }

    /**
     * Create an {@link IngestCoordinator} object that is configured using Sleeper {@link InstanceProperties} and {@link
     * TableProperties}.
     *
     * @param objectFactory            The object factory to use to create Sleeper iterators
     * @param sleeperStateStore        The state store to update with the new data
     * @param instanceProperties       The instance properties to use to configure the ingest
     * @param tableProperties          The table properties to use to configure the ingest
     * @param localWorkingDirectory    A local directory for temporary files
     * @param bufferAllocator          A buffer allocator to use during Arrow-based ingest. It may be null, but if it is
     *                                 needed, a {@link NullPointerException} will be thrown
     * @param s3AsyncClient            A client to use during asynchronous ingest. It may be null, but if it is needed,
     *                                 a {@link NullPointerException} will be thrown
     * @param hadoopConfiguration      An Hadoop configuration to use when writing Parquet files. It may be null, and if
     *                                 it is, a default configuration will be used
     * @param sleeperIteratorClassName The name of the Sleeper iterator to apply
     * @param sleeperIteratorConfig    The configuration of the iterator
     * @return The relevant ingest coordinator
     */
    public static IngestCoordinator<Record> createIngestCoordinatorFromProperties(
            ObjectFactory objectFactory,
            StateStore sleeperStateStore,
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            String localWorkingDirectory,
            BufferAllocator bufferAllocator,
            S3AsyncClient s3AsyncClient,
            Configuration hadoopConfiguration,
            String sleeperIteratorClassName,
            String sleeperIteratorConfig) {
        Schema sleeperSchema = tableProperties.getSchema();
        Supplier<RecordBatch<Record>> recordBatchFactoryFn;
        Function<Partition, PartitionFileWriter> partitionFileFactoryFn;
        // Define a factory function for record batches
        switch (instanceProperties.get(INGEST_RECORD_BATCH_TYPE).toLowerCase(Locale.ROOT)) {
            case "arraylist":
                recordBatchFactoryFn = () -> new ArrayListRecordBatchAcceptingRecords(
                        sleeperSchema,
                        localWorkingDirectory,
                        instanceProperties.getInt(MAX_IN_MEMORY_BATCH_SIZE),
                        instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY),
                        tableProperties.getInt(ROW_GROUP_SIZE),
                        tableProperties.getInt(PAGE_SIZE),
                        tableProperties.get(COMPRESSION_CODEC),
                        hadoopConfiguration);
                break;
            case "arrow":
                recordBatchFactoryFn = () ->
                        new ArrowRecordBatchAcceptingRecords(
                                bufferAllocator,
                                sleeperSchema,
                                localWorkingDirectory,
                                instanceProperties.getLong(ARROW_INGEST_WORKING_BUFFER_BYTES),
                                instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES),
                                instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES),
                                instanceProperties.getLong(ARROW_INGEST_MAX_LOCAL_STORE_BYTES),
                                instanceProperties.getInt(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS));
                break;
            default:
                throw new UnsupportedOperationException(String.format("Record batch type %s not supported", instanceProperties.get(INGEST_RECORD_BATCH_TYPE)));
        }
        // Define a factory function for partition file writers
        switch (instanceProperties.get(INGEST_PARTITION_FILE_WRITER_TYPE).toLowerCase(Locale.ROOT)) {
            case "direct":
                partitionFileFactoryFn = partition -> {
                    try {
                        return new DirectPartitionFileWriter(
                                sleeperSchema,
                                partition,
                                tableProperties.getInt(ROW_GROUP_SIZE),
                                tableProperties.getInt(PAGE_SIZE),
                                tableProperties.get(COMPRESSION_CODEC),
                                hadoopConfiguration,
                                instanceProperties.get(FILE_SYSTEM) + tableProperties.get(DATA_BUCKET));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                break;
            case "async":
                if (!instanceProperties.get(FILE_SYSTEM).toLowerCase(Locale.ROOT).equals("s3a://")) {
                    throw new UnsupportedOperationException("Attempting an asynchronous write to a file system that is not s3a://");
                }
                partitionFileFactoryFn = partition -> {
                    try {
                        return new AsyncS3PartitionFileWriter(
                                sleeperSchema,
                                partition,
                                tableProperties.getInt(ROW_GROUP_SIZE),
                                tableProperties.getInt(PAGE_SIZE),
                                tableProperties.get(COMPRESSION_CODEC),
                                hadoopConfiguration,
                                tableProperties.get(DATA_BUCKET),
                                s3AsyncClient,
                                localWorkingDirectory);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                break;
            default:
                throw new UnsupportedOperationException(String.format("Partition file writer type %s not supported", instanceProperties.get(INGEST_RECORD_BATCH_TYPE)));
        }
        return new IngestCoordinator<>(
                objectFactory,
                sleeperStateStore,
                sleeperSchema,
                sleeperIteratorClassName,
                sleeperIteratorConfig,
                instanceProperties.getInt(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS),
                recordBatchFactoryFn,
                partitionFileFactoryFn);
    }

    /**
     * Create a simple default Hadoop configuration which may be used if no other configuration is provided.
     *
     * @return The Hadoop configuration
     */
    private static Configuration defaultHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");
        return conf;
    }
}
