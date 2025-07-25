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
package sleeper.compaction.core.job.creation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.creation.strategy.CompactionStrategy;
import sleeper.compaction.core.job.creation.strategy.CompactionStrategyIndex;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.SplitFileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.util.SplitIntoBatches.splitListIntoBatchesOf;

/**
 * Creates compaction job definitions and posts them to an SQS queue. This is done as follows:
 * <p>- Queries the {@link StateStore} for active files which do not have a job id.
 * <p>- Groups these by partition.
 * <p>- For each partition, uses the configurable {@link CompactionStrategy} to decide what compaction jobs to create.
 * <p>- These compaction jobs are then sent to SQS.
 */
public class CreateCompactionJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateCompactionJobs.class);

    private final ObjectFactory objectFactory;
    private final InstanceProperties instanceProperties;
    private final BatchJobsWriter batchJobsWriter;
    private final BatchMessageSender batchMessageSender;
    private final StateStoreProvider stateStoreProvider;
    private final StateStoreCommitRequestSender stateStoreCommitSender;
    private final GenerateJobId generateJobId;
    private final GenerateBatchId generateBatchId;
    private final Random random;
    private final Supplier<Instant> timeSupplier;

    public CreateCompactionJobs(ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider,
            BatchJobsWriter batchJobsWriter,
            BatchMessageSender batchMessageSender,
            StateStoreCommitRequestSender stateStoreCommitSender,
            GenerateJobId generateJobId,
            GenerateBatchId generateBatchId,
            Random random,
            Supplier<Instant> timeSupplier) {
        this.objectFactory = objectFactory;
        this.instanceProperties = instanceProperties;
        this.batchJobsWriter = batchJobsWriter;
        this.batchMessageSender = batchMessageSender;
        this.stateStoreProvider = stateStoreProvider;
        this.stateStoreCommitSender = stateStoreCommitSender;
        this.generateJobId = generateJobId;
        this.generateBatchId = generateBatchId;
        this.random = random;
        this.timeSupplier = timeSupplier;
    }

    public void createJobsWithStrategy(TableProperties table) throws IOException, ObjectFactoryException {
        createJobs(table, false);
    }

    public void createJobWithForceAllFiles(TableProperties table) throws IOException, ObjectFactoryException {
        createJobs(table, true);
    }

    private void createJobs(TableProperties table, boolean forceAllFiles) throws IOException, ObjectFactoryException {
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        LOGGER.info("Performing pre-splits on files in table {}", table.getStatus());
        Instant preSplitStartTime = Instant.now();
        SplitFileReferences.from(stateStore).split();
        LOGGER.info("Pre-split files in partitions, took {}", LoggedDuration.withShortOutput(preSplitStartTime, Instant.now()));
        Instant finishTime = createJobsForTable(table, stateStore, forceAllFiles);
        LOGGER.info("Overall, pre-splitting files and creating compaction jobs took {}", LoggedDuration.withShortOutput(preSplitStartTime, finishTime));
    }

    private Instant createJobsForTable(TableProperties tableProperties, StateStore stateStore, boolean forceAllFiles) throws IOException, ObjectFactoryException {
        Instant startTime = Instant.now();
        TableStatus table = tableProperties.getStatus();
        LOGGER.info("Creating jobs for table {}", table);

        Instant loadPartitionStartTime = Instant.now();
        List<Partition> allPartitions = stateStore.getAllPartitions();

        // NB We retrieve the information about all the active file references and filter
        // that, rather than making separate calls to the state store for reasons
        // of efficiency and to ensure consistency.
        Instant loadFilesStartTime = Instant.now();
        List<FileReference> fileReferences = stateStore.getFileReferences();

        Instant compactionStrategyCreationStartTime = Instant.now();
        CompactionStrategy compactionStrategy = objectFactory
                .getObject(tableProperties.get(COMPACTION_STRATEGY_CLASS), CompactionStrategy.class);
        LOGGER.debug("Created compaction strategy of class {}", tableProperties.get(COMPACTION_STRATEGY_CLASS));
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties, generateJobId::generate);
        LOGGER.info("Initialised CompactionFactory with table {}, filename prefix {}",
                table, jobFactory.getOutputFilePrefix());

        Instant indexCreationStartTime = Instant.now();
        CompactionStrategyIndex index = new CompactionStrategyIndex(tableProperties.getStatus(), fileReferences, allPartitions);

        Instant jobCreationStartTime = Instant.now();
        List<CompactionJob> compactionJobs = compactionStrategy.createCompactionJobs(
                instanceProperties, tableProperties, jobFactory, index);
        LOGGER.info("Used {} to create {} compaction jobs for table {}", compactionStrategy.getClass().getSimpleName(), compactionJobs.size(), table);

        Instant leftoverJobCreationStartTime = Instant.now();
        if (forceAllFiles) {
            createJobsFromLeftoverFiles(tableProperties, jobFactory, fileReferences, allPartitions, compactionJobs);
        }

        Instant limitJobsStartTime = Instant.now();
        int creationLimit = tableProperties.getInt(TableProperty.COMPACTION_JOB_CREATION_LIMIT) - countCurrentRunningJobs(fileReferences);

        if (compactionJobs.size() > creationLimit) {
            compactionJobs = reduceCompactionJobsDownToCreationLimit(compactionJobs, creationLimit);
        }

        Instant sendJobsStartTime = Instant.now();
        AssignJobIdToFiles assignJobIdsToFiles;
        if (tableProperties.getBoolean(COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC)) {
            assignJobIdsToFiles = AssignJobIdToFiles.byQueue(stateStoreCommitSender);
        } else {
            assignJobIdsToFiles = AssignJobIdToFiles.synchronous(stateStore);
        }
        int sendBatchSize = tableProperties.getInt(COMPACTION_JOB_SEND_BATCH_SIZE);
        for (List<CompactionJob> batch : splitListIntoBatchesOf(sendBatchSize, compactionJobs)) {
            batchCreateJobs(assignJobIdsToFiles, tableProperties, batch);
        }
        Instant finishTime = Instant.now();

        LOGGER.info("Created {} compaction jobs, overall took {}", compactionJobs.size(), LoggedDuration.withShortOutput(startTime, finishTime));
        LOGGER.info("Loading partitions from state store took {}", LoggedDuration.withShortOutput(loadPartitionStartTime, loadFilesStartTime));
        LOGGER.info("Loading file references from state store took {}", LoggedDuration.withShortOutput(loadFilesStartTime, compactionStrategyCreationStartTime));
        LOGGER.info("Creating compaction strategy took {}", LoggedDuration.withShortOutput(compactionStrategyCreationStartTime, indexCreationStartTime));
        LOGGER.info("Creating compaction strategy index took {}", LoggedDuration.withShortOutput(indexCreationStartTime, jobCreationStartTime));
        LOGGER.info("Creating compaction jobs using strategy took {}", LoggedDuration.withShortOutput(jobCreationStartTime, leftoverJobCreationStartTime));
        LOGGER.info("Creating jobs from leftover files partitions took {}", LoggedDuration.withShortOutput(leftoverJobCreationStartTime, limitJobsStartTime));
        LOGGER.info("Limiting compaction jobs took {}", LoggedDuration.withShortOutput(limitJobsStartTime, sendJobsStartTime));
        LOGGER.info("Sending compaction jobs took {}", LoggedDuration.withShortOutput(sendJobsStartTime, finishTime));
        return finishTime;
    }

    private int countCurrentRunningJobs(List<FileReference> index) {
        return index.stream()
                .filter(file -> file.getJobId() != null)
                .map(FileReference::getJobId)
                .collect(Collectors.toSet())
                .size();
    }

    private List<CompactionJob> reduceCompactionJobsDownToCreationLimit(List<CompactionJob> compactionJobs, int creationLimit) {
        List<CompactionJob> outList = new ArrayList<CompactionJob>();

        IntStream.range(0, creationLimit)
                .forEach(loopIndex -> {
                    //Randomly select index of element from size of source array
                    int compactionIndexSelected = random.nextInt(compactionJobs.size());
                    //Copy job from the old compaction array into the new one and remove the selected on from the reference array
                    //so that it isn't selected again
                    outList.add(compactionJobs.get(compactionIndexSelected));
                    compactionJobs.remove(compactionIndexSelected);
                });

        return outList;
    }

    private void batchCreateJobs(AssignJobIdToFiles assignJobIdToFiles, TableProperties tableProperties, List<CompactionJob> compactionJobs) throws IOException {
        TableStatus tableStatus = tableProperties.getStatus();
        CompactionJobDispatchRequest request = CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                tableProperties, generateBatchId.generate(), timeSupplier.get());

        // Send batch of jobs to SQS (NB Send jobs to SQS before updating the job field of the files in the
        // StateStore so that if the send to SQS fails then the StateStore will not be updated and later another
        // job can be created for these files)
        LOGGER.debug("Writing batch of {} compaction jobs for table {} in data bucket at: {}", compactionJobs.size(), tableStatus, request.getBatchKey());
        Instant startTime = Instant.now();
        batchJobsWriter.writeJobs(instanceProperties.get(DATA_BUCKET), request.getBatchKey(), compactionJobs);
        LOGGER.debug("Sending compaction jobs batch, wrote jobs in {}",
                LoggedDuration.withShortOutput(startTime, Instant.now()));
        batchMessageSender.sendMessage(request);

        // Update the statuses of these files to record that a compaction job is in progress
        LOGGER.debug("Assigning input files for compaction jobs batch in table {}", tableStatus);
        assignJobIdToFiles.assignJobIds(compactionJobs.stream()
                .map(CompactionJob::createAssignJobIdRequest)
                .collect(Collectors.toList()), tableStatus);
        LOGGER.info("Created pending batch of {} compaction jobs for table {} in data bucket at: {}", compactionJobs.size(), tableStatus, request.getBatchKey());
    }

    private void createJobsFromLeftoverFiles(
            TableProperties tableProperties, CompactionJobFactory factory, List<FileReference> fileReferences,
            List<Partition> allPartitions, List<CompactionJob> compactionJobs) {
        List<FileReference> fileReferencesWithNoJobId = fileReferences.stream().filter(f -> null == f.getJobId()).collect(Collectors.toList());
        LOGGER.debug("Found {} file references with no job id in table {}", fileReferencesWithNoJobId.size(), tableProperties.getStatus());
        LOGGER.info("Creating compaction jobs for all files");
        int jobsBefore = compactionJobs.size();
        int batchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
        Set<String> leafPartitionIds = allPartitions.stream()
                .filter(Partition::isLeafPartition)
                .map(Partition::getId)
                .collect(Collectors.toSet());
        Set<String> assignedFiles = compactionJobs.stream()
                .flatMap(job -> job.getInputFiles().stream())
                .collect(Collectors.toSet());
        List<FileReference> leftoverFiles = fileReferencesWithNoJobId.stream()
                .filter(file -> !assignedFiles.contains(file.getFilename()))
                .collect(Collectors.toList());
        Map<String, List<FileReference>> filesByPartitionId = new HashMap<>();
        leftoverFiles.stream()
                .filter(fileReference -> leafPartitionIds.contains(fileReference.getPartitionId()))
                .forEach(fileReference -> filesByPartitionId.computeIfAbsent(
                        fileReference.getPartitionId(), key -> new ArrayList<>())
                        .add(fileReference));
        for (Partition partition : allPartitions) {
            List<FileReference> partitionFiles = filesByPartitionId.get(partition.getId());
            if (partitionFiles == null) {
                continue;
            }
            List<FileReference> filesForJob = new ArrayList<>();
            for (FileReference fileReference : partitionFiles) {
                filesForJob.add(fileReference);
                if (filesForJob.size() >= batchSize) {
                    LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                            filesForJob.size(), partition.getId(), tableProperties.get(TABLE_NAME));
                    compactionJobs.add(factory.createCompactionJob(filesForJob, partition.getId()));
                    filesForJob.clear();
                }
            }
            if (!filesForJob.isEmpty()) {
                LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                        filesForJob.size(), partition.getId(), tableProperties.get(TABLE_NAME));
                compactionJobs.add(factory.createCompactionJob(filesForJob, partition.getId()));
            }
        }
        LOGGER.info("Created {} jobs from {} leftover files for table {}",
                compactionJobs.size() - jobsBefore, leftoverFiles.size(), tableProperties.getStatus());
    }

    @FunctionalInterface
    public interface BatchJobsWriter {
        void writeJobs(String bucketName, String key, List<CompactionJob> compactionJobs);
    }

    @FunctionalInterface
    public interface BatchMessageSender {
        void sendMessage(CompactionJobDispatchRequest dispatchRequest);
    }

    @FunctionalInterface
    public interface GenerateJobId {
        String generate();

        static GenerateJobId random() {
            return () -> UUID.randomUUID().toString();
        }
    }

    @FunctionalInterface
    public interface GenerateBatchId {
        String generate();

        static GenerateBatchId random() {
            return () -> UUID.randomUUID().toString();
        }
    }
}
