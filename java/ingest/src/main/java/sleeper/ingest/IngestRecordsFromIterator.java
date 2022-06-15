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
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.Iterator;

/**
 * Writes an {@link Iterator} of {@link Record} objects to the storage system, partitioned and sorted.
 * <p>
 * This class is an adaptor to {@link IngestCoordinator}.
 */
public class IngestRecordsFromIterator {
    private final ObjectFactory objectFactory;
    private final Iterator<Record> recordsIterator;
    private final String localDir;
    private final long maxRecordsToWriteLocally;
    private final long maxInMemoryBatchSize;
    private final int rowGroupSize;
    private final int pageSize;
    private final String compressionCodec;
    private final StateStore stateStore;
    private final Schema schema;
    private final String fs;
    private final String bucketName;
    private final String iteratorClassName;
    private final String iteratorConfig;
    private final int ingestPartitionRefreshFrequencyInSeconds;
    Configuration hadoopConfiguraration;

    /**
     * This version of the constructor allows a bespoke Hadoop configuration to be specified. The underlying {@link
     * FileSystem} object maintains a cache of file systems and the first time that it creates a {@link
     * org.apache.hadoop.fs.s3a.S3AFileSystem} object, the provided Hadoop configuration will be used. Thereafter, the
     * Hadoop configuration will be ignored until {@link FileSystem#closeAll()} is called. This is not ideal behaviour.
     */
    public IngestRecordsFromIterator(ObjectFactory objectFactory,
                                     Iterator<Record> recordsIterator,
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
                                     Configuration hadoopConfiguraration) {
        this.objectFactory = objectFactory;
        this.recordsIterator = recordsIterator;
        this.localDir = localDir;
        this.maxRecordsToWriteLocally = maxRecordsToWriteLocally;
        this.maxInMemoryBatchSize = maxInMemoryBatchSize;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.compressionCodec = compressionCodec;
        this.stateStore = stateStore;
        this.schema = schema;
        this.fs = fs;
        this.bucketName = bucketName;
        this.iteratorClassName = iteratorClassName;
        this.iteratorConfig = iteratorConfig;
        this.ingestPartitionRefreshFrequencyInSeconds = ingestPartitionRefreshFrequencyInSeconds;
        this.hadoopConfiguraration = hadoopConfiguraration;
    }

    public IngestRecordsFromIterator(ObjectFactory objectFactory,
                                     Iterator<Record> recordsIterator,
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
        this(objectFactory, recordsIterator, localDir, maxRecordsToWriteLocally, maxInMemoryBatchSize, rowGroupSize,
                pageSize, compressionCodec, stateStore, schema, fs, bucketName, iteratorClassName, iteratorConfig,
                ingestPartitionRefreshFrequencyInSeconds, null);
    }

    public long write() throws StateStoreException, IOException, InterruptedException, IteratorException {
        IngestRecords ingestRecords = (hadoopConfiguraration == null) ?
                new IngestRecords(objectFactory, localDir,
                        maxRecordsToWriteLocally, maxInMemoryBatchSize, rowGroupSize, pageSize, compressionCodec, stateStore,
                        schema, fs, bucketName, iteratorClassName, iteratorConfig, ingestPartitionRefreshFrequencyInSeconds) :
                new IngestRecords(objectFactory, localDir,
                        maxRecordsToWriteLocally, maxInMemoryBatchSize, rowGroupSize, pageSize, compressionCodec, stateStore,
                        schema, fs, bucketName, iteratorClassName, iteratorConfig, ingestPartitionRefreshFrequencyInSeconds,
                        hadoopConfiguraration);
        ingestRecords.init();
        while (recordsIterator.hasNext()) {
            ingestRecords.write(recordsIterator.next());
        }
        return ingestRecords.close();
    }
}
