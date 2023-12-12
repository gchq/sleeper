/*
 * Copyright 2022-2023 Crown Copyright
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
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIdentity;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Creates compaction job definitions and posts them to an SQS queue.
 * <p>
 * This is done as follows:
 * - Queries the {@link StateStore} for active files which do not have a job id.
 * - Groups these by partition.
 * - For each partition, uses the configurable {@link CompactionStrategy} to
 * decide what compaction jobs to create.
 * - These compaction jobs are then sent to SQS.
 */
public class CreateJobs {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateJobs.class);

    private final ObjectFactory objectFactory;
    private final InstanceProperties instanceProperties;
    private final JobSender jobSender;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final boolean forceCreateJobs;

    private CreateJobs(ObjectFactory objectFactory,
                       InstanceProperties instanceProperties,
                       TablePropertiesProvider tablePropertiesProvider,
                       StateStoreProvider stateStoreProvider,
                       JobSender jobSender,
                       CompactionJobStatusStore jobStatusStore,
                       boolean forceCreateJobs) {
        this.objectFactory = objectFactory;
        this.instanceProperties = instanceProperties;
        this.jobSender = jobSender;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.jobStatusStore = jobStatusStore;
        this.forceCreateJobs = forceCreateJobs;
    }

    public static CreateJobs forceCompaction(ObjectFactory objectFactory,
                                             InstanceProperties instanceProperties,
                                             TablePropertiesProvider tablePropertiesProvider,
                                             StateStoreProvider stateStoreProvider,
                                             JobSender jobSender,
                                             CompactionJobStatusStore jobStatusStore) {
        return new CreateJobs(objectFactory, instanceProperties, tablePropertiesProvider, stateStoreProvider, jobSender, jobStatusStore, true);
    }

    public static CreateJobs standard(ObjectFactory objectFactory,
                                      InstanceProperties instanceProperties,
                                      TablePropertiesProvider tablePropertiesProvider,
                                      StateStoreProvider stateStoreProvider,
                                      JobSender jobSender,
                                      CompactionJobStatusStore jobStatusStore) {
        return new CreateJobs(objectFactory, instanceProperties, tablePropertiesProvider, stateStoreProvider, jobSender, jobStatusStore, false);
    }

    public void createJobs() throws StateStoreException, IOException, ObjectFactoryException {
        List<TableProperties> tables = tablePropertiesProvider.streamAllTables()
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found {} tables", tables.size());
        for (TableProperties table : tables) {
            createJobsForTable(table);
        }
    }

    public void createJobsForTable(TableProperties tableProperties) throws StateStoreException, IOException, ObjectFactoryException {
        TableIdentity tableId = tableProperties.getId();
        LOGGER.debug("Creating jobs for table {}", tableId);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);

        List<Partition> allPartitions = stateStore.getAllPartitions();

        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        // NB We retrieve the information about all the active files and filter
        // that, rather than making separate calls to the state store for reasons
        // of efficiency and to ensure consistency.
        List<FileInfo> activeFileInfosWithNoJobId = activeFiles.stream().filter(f -> null == f.getJobId()).collect(Collectors.toList());
        List<FileInfo> activeFileInfosWithJobId = activeFiles.stream().filter(f -> null != f.getJobId()).collect(Collectors.toList());
        LOGGER.debug("Found {} active files with no job id in table {}", activeFileInfosWithNoJobId.size(), tableId);
        LOGGER.debug("Found {} active files with a job id in table {}", activeFileInfosWithJobId.size(), tableId);

        CompactionStrategy compactionStrategy = objectFactory
                .getObject(tableProperties.get(COMPACTION_STRATEGY_CLASS), CompactionStrategy.class);
        LOGGER.debug("Created compaction strategy of class {}", tableProperties.get(COMPACTION_STRATEGY_CLASS));
        compactionStrategy.init(instanceProperties, tableProperties);

        List<CompactionJob> compactionJobs = compactionStrategy.createCompactionJobs(activeFileInfosWithJobId, activeFileInfosWithNoJobId, allPartitions);
        LOGGER.info("Used {} to create {} compaction jobs for table {}", compactionStrategy.getClass().getSimpleName(), compactionJobs.size(), tableId);
        if (forceCreateJobs) {
            LOGGER.info("Compacting leftover files");
            Set<String> leafPartitionIds = stateStore.getLeafPartitions().stream()
                    .map(Partition::getId)
                    .collect(Collectors.toSet());
            Set<String> assignedFiles = compactionJobs.stream().flatMap(job -> job.getInputFiles().stream()).collect(Collectors.toSet());
            List<FileInfo> leftoverFiles = activeFileInfosWithNoJobId.stream()
                    .filter(file -> !assignedFiles.contains(file.getFilename()))
                    .filter(FileInfo::onlyContainsDataForThisPartition)
                    .collect(Collectors.toList());
            int batchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
            Map<String, List<FileInfo>> filesByPartitionId = new HashMap<>();
            leftoverFiles.stream()
                    .filter(fileInfo -> leafPartitionIds.contains(fileInfo.getPartitionId()))
                    .forEach(fileInfo -> filesByPartitionId.computeIfAbsent(fileInfo.getPartitionId(), (key) -> new ArrayList<>()).add(fileInfo));
            CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
            for (Map.Entry<String, List<FileInfo>> fileByPartitionId : filesByPartitionId.entrySet()) {
                List<FileInfo> filesForJob = new ArrayList<>();
                for (FileInfo fileInfo : fileByPartitionId.getValue()) {
                    filesForJob.add(fileInfo);
                    if (filesForJob.size() >= batchSize) {
                        LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                                filesForJob.size(), fileByPartitionId.getKey(), tableProperties.get(TABLE_NAME));
                        compactionJobs.add(factory.createCompactionJob(filesForJob, fileByPartitionId.getKey()));
                        filesForJob.clear();
                    }
                }
                if (!filesForJob.isEmpty()) {
                    LOGGER.info("Creating a job to compact {} files in partition {} in table {}",
                            filesForJob.size(), fileByPartitionId.getKey(), tableProperties.get(TABLE_NAME));
                    compactionJobs.add(factory.createCompactionJob(filesForJob, fileByPartitionId.getKey()));
                }
            }
        }
        for (CompactionJob compactionJob : compactionJobs) {
            // Send compaction job to SQS (NB Send compaction job to SQS before updating the job field of the files in the
            // StateStore so that if the send to SQS fails then the StateStore will not be updated and later another
            // job can be created for these files.)
            jobSender.send(compactionJob);

            // Update the statuses of these files to record that a compaction job is in progress
            LOGGER.debug("Updating status of files in StateStore");

            List<FileInfo> fileInfos1 = new ArrayList<>();
            for (String filename : compactionJob.getInputFiles()) {
                for (FileInfo fileInfo : activeFiles) {
                    if (fileInfo.getPartitionId().equals(compactionJob.getPartitionId())
                            && fileInfo.getFilename().equals(filename)) {
                        fileInfos1.add(fileInfo);
                        break;
                    }
                }
            }
            stateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos1);
            jobStatusStore.jobCreated(compactionJob);
        }
    }

    @FunctionalInterface
    public interface JobSender {
        void send(CompactionJob compactionJob) throws IOException;
    }
}
