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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;

/**
 * Writes a {@link Record} objects to the storage system, partitioned and sorted.
 * <p>
 * This class is an adaptor to {@link IngestCoordinator}.
 */
public class IngestRecords {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRecords.class);

    private final IngestCoordinator<Record> ingestCoordinator;

    public IngestRecords(ObjectFactory objectFactory,
                         String localDir,
                         long maxRecordsToWriteLocally,
                         long maxInMemoryBatchSize,
                         int rowGroupSize,
                         int pageSize,
                         String compressionCodec,
                         StateStore stateStore,
                         Schema schema,
                         String fs,
                         String bucketName,
                         String iteratorClassName,
                         String iteratorConfig,
                         int ingestPartitionRefreshFrequencyInSeconds) {
        this(objectFactory,
                localDir,
                maxRecordsToWriteLocally,
                maxInMemoryBatchSize,
                rowGroupSize,
                pageSize,
                compressionCodec,
                stateStore,
                schema,
                fs,
                bucketName,
                iteratorClassName,
                iteratorConfig,
                ingestPartitionRefreshFrequencyInSeconds,
                defaultHadoopConfiguration());
    }

    /**
     * This version of the constructor allows a bespoke Hadoop configuration to be specified. The underlying {@link
     * FileSystem} object maintains a cache of file systems and the first time that it creates a {@link
     * org.apache.hadoop.fs.s3a.S3AFileSystem} object, the provided Hadoop configuration will be used. Thereafter, the
     * Hadoop configuration will be ignored until {@link FileSystem#closeAll()} is called. This is not ideal behaviour.
     */
    public IngestRecords(ObjectFactory objectFactory,
                         String localDir,
                         long maxRecordsToWriteLocally,
                         long maxInMemoryBatchSize,
                         int rowGroupSize,
                         int pageSize,
                         String compressionCodec,
                         StateStore stateStore,
                         Schema schema,
                         String fs,
                         String bucketName,
                         String iteratorClassName,
                         String iteratorConfig,
                         int ingestPartitionRefreshFrequencyInSeconds,
                         Configuration hadoopConfiguration) {
        try {
            this.ingestCoordinator = StandardIngestCoordinator.directWriteBackedByArrayList(
                    objectFactory,
                    stateStore,
                    schema,
                    localDir,
                    rowGroupSize,
                    pageSize,
                    compressionCodec,
                    hadoopConfiguration,
                    iteratorClassName,
                    iteratorConfig,
                    ingestPartitionRefreshFrequencyInSeconds,
                    fs + bucketName,
                    (int) maxInMemoryBatchSize,
                    maxRecordsToWriteLocally);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Configuration defaultHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");
        return conf;
    }

    public void init() throws StateStoreException {
        // Do nothing
    }

    public void write(Record record) throws IOException, IteratorException, InterruptedException, StateStoreException {
        ingestCoordinator.write(record);
    }

    public long close() throws IOException, IteratorException, InterruptedException, StateStoreException {
        return ingestCoordinator.closeReturningFileInfoList().stream()
                .mapToLong(FileInfo::getNumberOfRecords)
                .sum();
    }
}
