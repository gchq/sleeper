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
package sleeper.systemtest.compaction;

import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.systemtest.util.InvokeLambda;
import sleeper.systemtest.util.WaitForQueueEstimateNotEmpty;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class ApplyPartitionSplitAndWaitForCompletion {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplyPartitionSplitAndWaitForCompletion.class);

    private final String instanceId;
    private final String tableName;
    private final CompactionJobStatusStore store;
    private final WaitForPartitionSplitting waitForSplitting;
    private final WaitForCompactionJobs waitForCompaction;
    private final WaitForQueueEstimateNotEmpty waitForJobQueueEstimate;

    public ApplyPartitionSplitAndWaitForCompletion(
            AmazonSQS sqsClient, CompactionJobStatusStore store,
            InstanceProperties instanceProperties, String tableName) {
        this.instanceId = instanceProperties.get(ID);
        this.tableName = tableName;
        this.store = store;
        waitForSplitting = new WaitForPartitionSplitting(sqsClient, instanceProperties);
        waitForCompaction = new WaitForCompactionJobs(store, tableName);
        waitForJobQueueEstimate = new WaitForQueueEstimateNotEmpty(
                sqsClient, instanceProperties, SPLITTING_COMPACTION_JOB_QUEUE_URL);
    }

    /**
     * @return true if any splitting was done, false if none was needed
     */
    public boolean checkIfSplittingNeededAndWait() throws InterruptedException, IOException {
        LOGGER.info("Waiting for partition splits");
        waitForSplitting.pollUntilFinished();
        LOGGER.info("Creating compaction jobs");
        InvokeLambda.forInstance(instanceId, COMPACTION_JOB_CREATION_LAMBDA_FUNCTION);
        if (store.getUnfinishedJobs(tableName).isEmpty()) {
            LOGGER.info("Lambda created no more jobs, splitting complete");
            return false;
        }
        // SQS message count doesn't always seem to update before task creation Lambda runs, so wait for it
        // (the Lambda decides how many tasks to run based on how many messages it can see are in the queue)
        waitForJobQueueEstimate.pollUntilFinished();
        LOGGER.info("Lambda created new jobs, creating splitting compaction tasks");
        InvokeLambda.forInstance(instanceId, SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION);
        waitForCompaction.pollUntilFinished();
        return true;
    }

    public static boolean run(AmazonSQS sqsClient, CompactionJobStatusStore store,
                              InstanceProperties instanceProperties, String tableName)
            throws IOException, InterruptedException {
        return new ApplyPartitionSplitAndWaitForCompletion(sqsClient, store, instanceProperties, tableName)
                .checkIfSplittingNeededAndWait();
    }

}
