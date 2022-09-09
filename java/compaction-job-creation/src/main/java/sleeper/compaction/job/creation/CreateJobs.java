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
package sleeper.compaction.job.creation;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.compaction.strategy.CompactionStrategy;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.table.job.TableLister;
import sleeper.table.util.StateStoreProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;

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
    private final ObjectFactory objectFactory;
    private final InstanceProperties instanceProperties;
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateJobs.class);
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonSQS sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobSerDe compactionJobSerDe;
    private final TableLister tableLister;

    public CreateJobs(ObjectFactory objectFactory,
                      InstanceProperties instanceProperties,
                      TablePropertiesProvider tablePropertiesProvider,
                      StateStoreProvider stateStoreProvider,
                      AmazonDynamoDB dynamoDBClient,
                      AmazonSQS sqsClient,
                      TableLister tableLister) {
        this.objectFactory = objectFactory;
        this.instanceProperties = instanceProperties;
        this.dynamoDBClient = dynamoDBClient;
        this.sqsClient = sqsClient;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
        this.tableLister = tableLister;
    }

    public void createJobs() throws StateStoreException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, ObjectFactoryException {
        List<String> tables = tableLister.listTables();
        LOGGER.debug("Found {} tables", tables.size());
        for (String table : tables) {
            createJobsForTable(table);
        }
    }

    public void createJobsForTable(String tableName) throws StateStoreException, IOException, ObjectFactoryException {
        LOGGER.debug("Creating jobs for table {}", tableName);
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
        StateStore stateStore = stateStoreProvider.getStateStore(tableName, tablePropertiesProvider);

        List<Partition> allPartitions = stateStore.getAllPartitions();

        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        // NB We retrieve the information about all the active files and filter
        // that, rather than making separate calls to the state store for reasons
        // of efficiency and to ensure consistency.
        List<FileInfo> activeFileInfosWithNoJobId = activeFiles.stream().filter(f -> null == f.getJobId()).collect(Collectors.toList());
        List<FileInfo> activeFileInfosWithJobId = activeFiles.stream().filter(f -> null != f.getJobId()).collect(Collectors.toList());
        LOGGER.debug("Found {} active files with no job id", activeFileInfosWithNoJobId.size());
        LOGGER.debug("Found {} active files with a job id", activeFileInfosWithJobId.size());

        CompactionStrategy compactionStrategy = objectFactory
                .getObject(tableProperties.get(COMPACTION_STRATEGY_CLASS), CompactionStrategy.class);
        LOGGER.debug("Created compaction strategy of class {}", tableProperties.get(COMPACTION_STRATEGY_CLASS));
        compactionStrategy.init(instanceProperties, tableProperties);

        List<CompactionJob> compactionJobs = compactionStrategy.createCompactionJobs(activeFileInfosWithJobId, activeFileInfosWithNoJobId, allPartitions);
        LOGGER.info("Used {} to create {} compaction jobs", compactionStrategy.getClass().getSimpleName(), compactionJobs.size());

        for (CompactionJob compactionJob : compactionJobs) {
            // Send compaction job to SQS (NB Send compaction job to SQS before updating the job field of the files in the
            // StateStore so that if the send to SQS fails then the StateStore will not be updated and later another
            // job can be created for these files.)
            if (compactionJob.isSplittingJob()) {
                sendCompactionJobToSQS(compactionJob, instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL));
            } else {
                sendCompactionJobToSQS(compactionJob, instanceProperties.get(COMPACTION_JOB_QUEUE_URL));
            }

            // Update the statuses of these files to record that a compaction job is in progress
            LOGGER.debug("Updating status of files in StateStore");

            List<FileInfo> fileInfos1 = new ArrayList<>();
            for (String filename : compactionJob.getInputFiles()) {
                for (FileInfo fileInfo : activeFiles) {
                    if (fileInfo.getFilename().equals(filename)) {
                        fileInfos1.add(fileInfo);
                        break;
                    }
                }
            }
            stateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos1);
        }
    }

    private void sendCompactionJobToSQS(CompactionJob compactionJob,
                                        String queueUrl) throws IOException {
        String serialisedJobDefinition = compactionJobSerDe.serialiseToString(compactionJob);
        LOGGER.debug("Sending compaction job with id {} to SQS", compactionJob.getId());
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(serialisedJobDefinition);
        SendMessageResult sendMessageResult = sqsClient.sendMessage(sendMessageRequest);
        LOGGER.debug("Result of sending message: {}", sendMessageResult);
    }
}
