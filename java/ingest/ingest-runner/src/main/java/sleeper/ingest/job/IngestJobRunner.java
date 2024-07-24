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
package sleeper.ingest.job;

import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.ingest.IngestResult;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.commit.AddFilesToStateStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.utils.HadoopPathUtils;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;

/**
 * An IngestJobRunner takes ingest jobs and runs them.
 */
public class IngestJobRunner implements IngestJobHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobRunner.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final String fs;
    private final Configuration hadoopConfiguration;
    private final String taskId;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobStatusStore statusStore;
    private final AmazonSQS sqsClient;
    private final IngestFactory ingestFactory;
    private final PropertiesReloader propertiesReloader;
    private final Supplier<Instant> timeSupplier;

    public IngestJobRunner(ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            PropertiesReloader propertiesReloader,
            StateStoreProvider stateStoreProvider,
            IngestJobStatusStore statusStore,
            String taskId,
            String localDir,
            S3AsyncClient s3AsyncClient,
            AmazonSQS sqsClient,
            Configuration hadoopConfiguration,
            Supplier<Instant> timeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.propertiesReloader = propertiesReloader;
        this.fs = instanceProperties.get(FILE_SYSTEM);
        this.hadoopConfiguration = hadoopConfiguration;
        this.taskId = taskId;
        this.stateStoreProvider = stateStoreProvider;
        this.statusStore = statusStore;
        this.sqsClient = sqsClient;
        this.timeSupplier = timeSupplier;
        this.ingestFactory = IngestFactory.builder()
                .objectFactory(objectFactory)
                .localDir(localDir)
                .stateStoreProvider(stateStoreProvider)
                .hadoopConfiguration(hadoopConfiguration)
                .instanceProperties(instanceProperties)
                .s3AsyncClient(s3AsyncClient)
                .build();
    }

    @Override
    public IngestResult ingest(IngestJob job, String jobRunId) throws IteratorCreationException, StateStoreException, IOException {
        propertiesReloader.reloadIfNeeded();
        TableProperties tableProperties = tablePropertiesProvider.getByName(job.getTableName());
        Schema schema = tableProperties.getSchema();

        // Create list of all files to be read
        List<Path> paths = HadoopPathUtils.getPaths(job.getFiles(), hadoopConfiguration, fs);
        LOGGER.info("There are {} files to ingest", paths.size());
        LOGGER.debug("Files to ingest are: {}", paths);

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

        // Concatenate iterators into one iterator and run the ingest
        IngestResult result;
        try (CloseableIterator<Record> concatenatingIterator = new ConcatenatingIterator(inputIterators);
                IngestCoordinator<Record> ingestCoordinator = ingestFactory.ingestCoordinatorBuilder(tableProperties)
                        .addFilesToStateStore(addFilesToStateStore(job, jobRunId, tableProperties))
                        .build()) {
            result = new IngestRecordsFromIterator(ingestCoordinator, concatenatingIterator).write();
        }
        LOGGER.info("Ingest job {}: Wrote {} records from files {}", job.getId(), result.getRecordsWritten(), paths);
        return result;
    }

    private AddFilesToStateStore addFilesToStateStore(IngestJob job, String jobRunId, TableProperties tableProperties) {
        if (tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC)) {
            return AddFilesToStateStore.bySqs(sqsClient, instanceProperties,
                    requestBuilder -> requestBuilder.ingestJob(job).taskId(taskId).jobRunId(jobRunId).writtenTime(timeSupplier.get()));
        } else {
            return AddFilesToStateStore.synchronous(stateStoreProvider.getStateStore(tableProperties), statusStore,
                    updateBuilder -> updateBuilder.job(job).taskId(taskId).jobRunId(jobRunId).writtenTime(timeSupplier.get()));
        }
    }
}
