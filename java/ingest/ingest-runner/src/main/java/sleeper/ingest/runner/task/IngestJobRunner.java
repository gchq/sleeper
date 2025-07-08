/*
 * Copyright 2022-2025 Crown Copyright
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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.utils.S3ExpandDirectories;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobHandler;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.IngestRecordsFromIterator;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * An IngestJobRunner takes ingest jobs and runs them.
 */
public class IngestJobRunner implements IngestJobHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobRunner.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    //private final String fs;
    private final Configuration hadoopConfiguration;
    private final String taskId;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobTracker tracker;
    private final IngestFactory ingestFactory;
    private final StateStoreCommitRequestSender commitSender;
    private final PropertiesReloader propertiesReloader;
    private final Supplier<Instant> timeSupplier;
    private final S3ExpandDirectories expandDirectories;

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    public IngestJobRunner(ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            PropertiesReloader propertiesReloader,
            StateStoreProvider stateStoreProvider,
            IngestJobTracker tracker,
            String taskId,
            String localDir,
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            SqsClient sqsClient,
            Configuration hadoopConfiguration,
            Supplier<Instant> timeSupplier) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.propertiesReloader = propertiesReloader;
        this.hadoopConfiguration = hadoopConfiguration;
        this.taskId = taskId;
        this.stateStoreProvider = stateStoreProvider;
        this.tracker = tracker;
        this.timeSupplier = timeSupplier;
        this.ingestFactory = IngestFactory.builder()
                .objectFactory(objectFactory)
                .localDir(localDir)
                .stateStoreProvider(stateStoreProvider)
                .hadoopConfiguration(hadoopConfiguration)
                .instanceProperties(instanceProperties)
                .s3AsyncClient(s3AsyncClient)
                .build();
        this.expandDirectories = new S3ExpandDirectories(s3Client);
        this.commitSender = new SqsFifoStateStoreCommitRequestSender(
                instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));
    }

    @Override
    public IngestResult ingest(IngestJob job, String jobRunId) throws IteratorCreationException, StateStoreException, IOException {
        propertiesReloader.reloadIfNeeded();
        TableProperties tableProperties = tablePropertiesProvider.getByName(job.getTableName());
        Schema schema = tableProperties.getSchema();

        // Create list of all files to be read

        List<String> paths = expandDirectories.expandPaths(job.getFiles()).listHadoopPathsThrowIfAnyPathIsEmpty();
        LOGGER.info("There are {} files to ingest", paths.size());
        LOGGER.debug("Files to ingest are: {}", paths);

        // Create supplier of iterator of records from each file (using a supplier avoids having multiple files open
        // at the same time)
        List<Supplier<CloseableIterator<SleeperRow>>> inputIterators = new ArrayList<>();
        for (String path : paths) {
            if (path.endsWith(".parquet")) {
                inputIterators.add(() -> {
                    try {
                        ParquetReader<SleeperRow> reader = new ParquetRecordReader.Builder(path, schema).withConf(hadoopConfiguration).build();
                        return new ParquetReaderIterator(reader);
                    } catch (IOException e) {
                        throw new RuntimeException("Ingest job: " + job.getId() + " IOException creating reader for file "
                                + path + ": " + e.getMessage());
                    }
                });
            } else {
                LOGGER.error("A file with a currently unsupported format has been found on ingest, file path: {}"
                        + ". This file will be ignored and will not be ingested.", path);
            }
        }

        // Concatenate iterators into one iterator and run the ingest
        IngestResult result;
        try (CloseableIterator<SleeperRow> concatenatingIterator = new ConcatenatingIterator(inputIterators);
                IngestCoordinator<SleeperRow> ingestCoordinator = ingestFactory.ingestCoordinatorBuilder(tableProperties)
                        .addFilesToStateStore(addFilesToStateStore(job, jobRunId, tableProperties))
                        .build()) {
            result = new IngestRecordsFromIterator(ingestCoordinator, concatenatingIterator).write();
        }
        LOGGER.info("Ingest job {}: Wrote {} records from files {}", job.getId(), result.getRecordsWritten(), paths);
        return result;
    }

    private AddFilesToStateStore addFilesToStateStore(IngestJob job, String jobRunId, TableProperties tableProperties) {
        IngestJobRunIds runIds = IngestJobRunIds.builder().tableId(tableProperties.get(TABLE_ID)).jobId(job.getId()).taskId(taskId).jobRunId(jobRunId).build();
        if (tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC)) {
            return AddFilesToStateStore.asynchronousWithJob(tableProperties, commitSender, timeSupplier, runIds);
        } else {
            return AddFilesToStateStore.synchronousWithJob(tableProperties, stateStoreProvider.getStateStore(tableProperties), tracker, timeSupplier, runIds);
        }
    }
}
