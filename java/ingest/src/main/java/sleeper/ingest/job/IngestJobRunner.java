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
package sleeper.ingest.job;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.ingest.IngestResult;
import sleeper.ingest.impl.IngestCoordinatorFactory;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;

/**
 * An IngestJobRunner takes ingest jobs and runs them.
 */
public class IngestJobRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobRunner.class);

    private final ObjectFactory objectFactory;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final String localDir;
    private final long maxLinesInLocalFile;
    private final long maxInMemoryBatchSize;
    private final String fs;
    private final S3AsyncClient s3AsyncClient;
    private final Configuration hadoopConfiguration;

    public IngestJobRunner(ObjectFactory objectFactory,
                           InstanceProperties instanceProperties,
                           TablePropertiesProvider tablePropertiesProvider,
                           StateStoreProvider stateStoreProvider,
                           String localDir,
                           S3AsyncClient s3AsyncClient,
                           Configuration hadoopConfiguration) {
        this.objectFactory = objectFactory;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.localDir = localDir;
        this.maxLinesInLocalFile = instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY);
        this.maxInMemoryBatchSize = instanceProperties.getLong(MAX_IN_MEMORY_BATCH_SIZE);
        this.fs = instanceProperties.get(FILE_SYSTEM);
        this.hadoopConfiguration = hadoopConfiguration;
        this.s3AsyncClient = s3AsyncClient;
    }

    public IngestResult ingest(IngestJob job) throws InterruptedException, IteratorException, StateStoreException, IOException {
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(job.getTableName());
        Schema schema = tableProperties.getSchema();

        // Create list of all files to be read
        List<Path> paths = IngestJobUtils.getPaths(job.getFiles(), hadoopConfiguration, fs);
        LOGGER.info("There are {} files to ingest", paths.size());
        LOGGER.debug("Files to ingest are: {}", paths);
        LOGGER.info("Max number of records to read into memory is {}", maxInMemoryBatchSize);
        LOGGER.info("Max number of records to write to local disk is {}", maxLinesInLocalFile);

        // Create supplier of iterator of records from each file (using a supplier avoids having multiple files open
        // at the same time)
        List<Supplier<CloseableIterator<Record>>> inputIterators = new ArrayList<>();
        for (Path path : paths) {
            String pathString = path.toString();
            if (pathString.endsWith(".parquet")) {
                inputIterators.add(() -> {
                    try {
                        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).withConf(hadoopConfiguration).build();
                        return new ParquetReaderIterator(reader);
                    } catch (IOException e) {
                        throw new RuntimeException("Ingest job: " + job.getId() + " IOException creating reader for file "
                                + path + ": " + e.getMessage());
                    }
                });
            } else {
                LOGGER.error("A file with a currently unsupported format has been found on ingest, file path: {}"
                        + ". This file will be ignored and will not be ingested.", pathString);
            }
        }
        // Get StateStore for this table
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        // Run the ingest
        try (CloseableIterator<Record> recordIterator = new ConcatenatingIterator(inputIterators);
             BufferAllocator bufferAllocator = new RootAllocator()) {

            IngestCoordinatorFactory factory = IngestCoordinatorFactory.builder()
                    .objectFactory(objectFactory)
                    .stateStore(stateStore)
                    .localDir(localDir)
                    .s3AsyncClient(s3AsyncClient)
                    .hadoopConfiguration(hadoopConfiguration)
                    .bufferAllocator(bufferAllocator)
                    .build();
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(
                    factory.createIngestCoordinator(instanceProperties, tableProperties), recordIterator);
            IngestResult result = ingestRecordsFromIterator.write();

            LOGGER.info("Ingest job {}: Wrote {} records from files {}", job.getId(), result.getNumberOfRecords(), paths);
            return result;
        }
    }
}
