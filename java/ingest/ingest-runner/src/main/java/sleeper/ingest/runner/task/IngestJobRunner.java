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
package sleeper.ingest.runner.task;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobHandler;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.IngestRecordsFromIterator;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.utils.HadoopPathUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;

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
    private final AmazonS3 s3Client;
    private final AmazonSQS sqsClient;
    private final IngestFactory ingestFactory;
    private final PropertiesReloader propertiesReloader;
    private final Supplier<Instant> timeSupplier;

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    public IngestJobRunner(ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            PropertiesReloader propertiesReloader,
            StateStoreProvider stateStoreProvider,
            IngestJobStatusStore statusStore,
            String taskId,
            String localDir,
            AmazonS3 s3Client,
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
        this.s3Client = s3Client;
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
            return AddFilesToStateStore.bySqs(sqsClient, s3Client, instanceProperties,
                    requestBuilder -> requestBuilder.ingestJob(job).taskId(taskId).jobRunId(jobRunId).writtenTime(timeSupplier.get()));
        } else {
            return AddFilesToStateStore.synchronous(stateStoreProvider.getStateStore(tableProperties), statusStore,
                    updateBuilder -> updateBuilder.job(job).taskId(taskId).jobRunId(jobRunId).writtenTime(timeSupplier.get()));
        }
    }
}
