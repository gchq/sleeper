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
package sleeper.compaction.job.creation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.strategy.CompactionStrategy;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.CompactionProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.SplitFileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Duration;
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

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_SEND_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
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
    private final JobSender jobSender;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final Mode mode;
    private final Supplier<String> jobIdSupplier;
    private final Random random;

    public CreateCompactionJobs(ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider,
            JobSender jobSender,
            CompactionJobStatusStore jobStatusStore,
            Mode mode) {
        this(objectFactory, instanceProperties, stateStoreProvider, jobSender, jobStatusStore, mode, () -> UUID.randomUUID().toString(), new Random());
    }

    public CreateCompactionJobs(ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider,
            JobSender jobSender,
            CompactionJobStatusStore jobStatusStore,
            Mode mode,
            Supplier<String> jobIdSupplier,
            Random random) {
        this.objectFactory = objectFactory;
        this.instanceProperties = instanceProperties;
        this.jobSender = jobSender;
        this.stateStoreProvider = stateStoreProvider;
        this.jobStatusStore = jobStatusStore;
        this.mode = mode;
        this.jobIdSupplier = jobIdSupplier;
        this.random = random;
    }

    public enum Mode {
        STRATEGY, FORCE_ALL_FILES_AFTER_STRATEGY
    }

    public void createJobs(TableProperties table) throws StateStoreException, IOException, ObjectFactoryException {
        LOGGER.info("Performing pre-splits on files in {}", table.getStatus());
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        SplitFileReferences.from(stateStore).split();
        createJobsForTable(table, stateStore);
    }

    private void createJobsForTable(TableProperties tableProperties, StateStore stateStore) throws StateStoreException, IOException, ObjectFactoryException {
        Instant startTime = Instant.now();
        TableStatus table = tableProperties.getStatus();
        LOGGER.debug("Creating jobs for table {}", table);

        Instant loadPartitionStartTime = Instant.now();
        List<Partition> allPartitions = stateStore.getAllPartitions();
        Duration loadPartitionDuration = Duration.between(loadPartitionStartTime, Instant.now());

        // NB We retrieve the information about all the active file references and filter
        // that, rather than making separate calls to the state store for reasons
        // of efficiency and to ensure consistency.
        Instant loadFilesStartTime = Instant.now();
        List<FileReference> fileReferences = stateStore.getFileReferences();
        Duration loadFilesDuration = Duration.between(loadFilesStartTime, Instant.now());

        Instant compactionStrategyCreationStartTime = Instant.now();
        CompactionStrategy compactionStrategy = objectFactory
                .getObject(tableProperties.get(COMPACTION_STRATEGY_CLASS), CompactionStrategy.class);
        Duration objectFactoryCreationDuration = Duration.between(compactionStrategyCreationStartTime, Instant.now());
        LOGGER.debug("Created compaction strategy of class {}", tableProperties.get(COMPACTION_STRATEGY_CLASS));
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties, jobIdSupplier);

        Instant jobCreationStartTime = Instant.now();
        List<CompactionJob> compactionJobs = compactionStrategy.createCompactionJobs(
                instanceProperties, tableProperties, jobFactory, fileReferences, allPartitions);
        Duration jobCreationDuration = Duration.between(jobCreationStartTime, Instant.now());
        LOGGER.info("Used {} to create {} compaction jobs for table {}", compactionStrategy.getClass().getSimpleName(), compactionJobs.size(), table);

        Duration leftoverJobDuration = null;
        if (mode == Mode.FORCE_ALL_FILES_AFTER_STRATEGY) {
            Instant leftoverJobCreationStartTime = Instant.now();
            createJobsFromLeftoverFiles(tableProperties, jobFactory, fileReferences, allPartitions, compactionJobs);
            leftoverJobDuration = Duration.between(leftoverJobCreationStartTime, Instant.now());
        }
        int creationLimit = instanceProperties.getInt(CompactionProperty.COMPACTION_JOB_CREATION_LIMIT);
        Duration limitJobsDuration = null;
        if (compactionJobs.size() > creationLimit) {
            Instant limitJobStartTime = Instant.now();
            compactionJobs = reduceCompactionJobsDownToCreationLimit(compactionJobs, creationLimit);
            limitJobsDuration = Duration.between(limitJobStartTime, Instant.now());
        }

        int sendBatchSize = tableProperties.getInt(COMPACTION_JOB_SEND_BATCH_SIZE);
        Instant batchCreateJobsStartTime = Instant.now();
        for (List<CompactionJob> batch : splitListIntoBatchesOf(sendBatchSize, compactionJobs)) {
            batchCreateJobs(stateStore, batch);
        }
        Instant finishTime = Instant.now();
        Duration batchCreateJobsDuration = Duration.between(batchCreateJobsStartTime, finishTime);

        LOGGER.info("Created {} compaction jobs, overall took {}", compactionJobs.size(), LoggedDuration.withShortOutput(startTime, finishTime));
        LOGGER.info("Loading partitions from state store took {}", LoggedDuration.withShortOutput(loadPartitionDuration));
        LOGGER.info("Loading file references from state store took {}", LoggedDuration.withShortOutput(loadFilesDuration));
        LOGGER.info("Creating compaction strategy took {}", LoggedDuration.withShortOutput(objectFactoryCreationDuration));
        LOGGER.info("Creating compaction strategy index took ", LoggedDuration.withShortOutput(compactionStrategy.getLoadIndexDuration()));
        LOGGER.info("Creating compaction jobs using strategy took {}", LoggedDuration.withShortOutput(jobCreationDuration));
        if (leftoverJobDuration != null) {
            LOGGER.info("Creating jobs from leftover files partitions took {}", LoggedDuration.withShortOutput(leftoverJobDuration));
        }
        if (limitJobsDuration != null) {
            LOGGER.info("Limiting compaction jobs took {}", LoggedDuration.withShortOutput(limitJobsDuration));
        }
        LOGGER.info("Batch creating jobs took {}", LoggedDuration.withShortOutput(batchCreateJobsDuration));
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

    private void batchCreateJobs(StateStore stateStore, List<CompactionJob> compactionJobs) throws StateStoreException, IOException {
        for (CompactionJob compactionJob : compactionJobs) {
            // Record job was created before we send it to SQS, otherwise this update can conflict with a compaction
            // task trying to record that the job was started.
            jobStatusStore.jobCreated(compactionJob);

            // Send compaction job to SQS (NB Send compaction job to SQS before updating the job field of the files in the
            // StateStore so that if the send to SQS fails then the StateStore will not be updated and later another
            // job can be created for these files.)
            jobSender.send(compactionJob);
        }
        // Update the statuses of these files to record that a compaction job is in progress
        LOGGER.debug("Updating status of files in StateStore");
        stateStore.assignJobIds(compactionJobs.stream()
                .map(job -> assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles()))
                .collect(Collectors.toList()));
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
                .forEach(fileReference -> filesByPartitionId.computeIfAbsent(fileReference.getPartitionId(),
                        (key) -> new ArrayList<>()).add(fileReference));
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
    public interface JobSender {
        void send(CompactionJob compactionJob) throws IOException;
    }
}
