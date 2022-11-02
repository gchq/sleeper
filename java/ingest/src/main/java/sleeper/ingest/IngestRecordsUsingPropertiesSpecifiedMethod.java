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
package sleeper.ingest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;

/**
 * This class provide methods to support ingest into Sleeper, where the way in which that ingest should take place is
 * specified in the instance properties and the table properties.
 */
public class IngestRecordsUsingPropertiesSpecifiedMethod {

    private IngestRecordsUsingPropertiesSpecifiedMethod() {
    }

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
        IngestProperties ingestProperties = createIngestProperties(objectFactory,
                sleeperStateStore, instanceProperties, tableProperties, localWorkingDirectory,
                internalHadoopConfiguration, sleeperIteratorClassName, sleeperIteratorConfig);
        try (BufferAllocator arrowBufferAllocator = (totalArrowBytesRequired > 0) ?
                new RootAllocator(totalArrowBytesRequired) : null;
             IngestCoordinator<Record> ingestCoordinator = createIngestCoordinatorWithProperties(
                     ingestProperties, instanceProperties, arrowBufferAllocator,
                     internalS3AsyncClient)) {
            return new IngestRecordsFromIterator(ingestCoordinator, recordIterator).write().getFileInfoList();
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
     * TableProperties}
     *
     * @param ingestProperties   The ingest properties to use to configure the ingest
     * @param instanceProperties The instance properties to use to configure the ingest
     * @param bufferAllocator    A buffer allocator to use during Arrow-based ingest. It may be null, but if it is
     *                           needed, a {@link NullPointerException} will be thrown
     * @param s3AsyncClient      A client to use during asynchronous ingest. It may be null, but if it is needed,
     *                           a {@link NullPointerException} will be thrown
     * @return The relevant IngestCoordinator object
     */
    public static IngestCoordinator<Record> createIngestCoordinatorWithProperties(
            IngestProperties ingestProperties,
            InstanceProperties instanceProperties,
            BufferAllocator bufferAllocator,
            S3AsyncClient s3AsyncClient) {
        // Define a factory function for record batches
        String recordBatchType = instanceProperties.get(INGEST_RECORD_BATCH_TYPE).toLowerCase(Locale.ROOT);
        String fileWriterType = instanceProperties.get(INGEST_PARTITION_FILE_WRITER_TYPE).toLowerCase(Locale.ROOT);
        StandardIngestCoordinator.BackedBuilder ingestCoordinatorBuilder;
        if (recordBatchType.equals("arraylist")) {
            ingestCoordinatorBuilder = StandardIngestCoordinator.builder().fromProperties(ingestProperties)
                    .backedByArrayList()
                    .maxNoOfRecordsInMemory((int) ingestProperties.getMaxInMemoryBatchSize())
                    .maxNoOfRecordsInLocalStore(ingestProperties.getMaxRecordsToWriteLocally());
        } else if (recordBatchType.equals("arrow")) {
            ingestCoordinatorBuilder = StandardIngestCoordinator.builder().fromProperties(ingestProperties)
                    .backedByArrow()
                    .arrowBufferAllocator(bufferAllocator)
                    .maxNoOfRecordsToWriteToArrowFileAtOnce(instanceProperties.getInt(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS))
                    .workingArrowBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_WORKING_BUFFER_BYTES))
                    .minBatchArrowBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxBatchArrowBufferAllocatorBytes(instanceProperties.getLong(ARROW_INGEST_BATCH_BUFFER_BYTES))
                    .maxNoOfBytesToWriteLocally(ingestProperties.getMaxRecordsToWriteLocally());
        } else {
            throw new UnsupportedOperationException(String.format("Record batch type %s not supported", recordBatchType));
        }
        if (fileWriterType.equals("direct")) {
            return ingestCoordinatorBuilder.buildDirectWrite(ingestProperties.getFilePrefix() + ingestProperties.getBucketName());
        } else if (fileWriterType.equals("async")) {
            if (!instanceProperties.get(FILE_SYSTEM).toLowerCase(Locale.ROOT).equals("s3a://")) {
                throw new UnsupportedOperationException("Attempting an asynchronous write to a file system that is not s3a://");
            } else {
                return ingestCoordinatorBuilder.buildAsyncS3Write(ingestProperties.getBucketName(), s3AsyncClient);
            }
        } else {
            throw new UnsupportedOperationException(String.format("Record batch type %s not supported", recordBatchType));
        }
    }

    private static IngestProperties createIngestProperties(ObjectFactory objectFactory,
                                                           StateStore sleeperStateStore,
                                                           InstanceProperties instanceProperties,
                                                           TableProperties tableProperties,
                                                           String localWorkingDirectory,
                                                           Configuration hadoopConfiguration,
                                                           String sleeperIteratorClassName,
                                                           String sleeperIteratorConfig) {
        return IngestProperties.builder()
                .objectFactory(objectFactory)
                .localDir(localWorkingDirectory)
                .rowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .pageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .stateStore(sleeperStateStore)
                .schema(tableProperties.getSchema())
                .iteratorClassName(sleeperIteratorClassName)
                .iteratorConfig(sleeperIteratorConfig)
                .compressionCodec(tableProperties.get(COMPRESSION_CODEC))
                .filePathPrefix(instanceProperties.get(FILE_SYSTEM))
                .bucketName(tableProperties.get(DATA_BUCKET))
                .hadoopConfiguration(hadoopConfiguration)
                .maxInMemoryBatchSize(instanceProperties.getInt(MAX_IN_MEMORY_BATCH_SIZE))
                .maxRecordsToWriteLocally(instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY))
                .ingestPartitionRefreshFrequencyInSecond(120).build();
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
