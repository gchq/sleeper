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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.util.InvokeSystemTestLambda;
import sleeper.systemtest.util.WaitForQueueEstimate;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;

public class InvokeCompactionJobCreation {

    private InvokeCompactionJobCreation() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        String instanceId = args[0];

        InvokeSystemTestLambda.forInstance(instanceId, COMPACTION_JOB_CREATION_LAMBDA_FUNCTION);

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(AmazonS3ClientBuilder.defaultClient(), instanceId);
        CompactionJobStatusStore statusStore = CompactionJobStatusStoreFactory.getStatusStore(
                AmazonDynamoDBClientBuilder.defaultClient(), systemTestProperties);

        WaitForQueueEstimate.containsUnfinishedJobs(AmazonSQSClientBuilder.defaultClient(),
                        systemTestProperties, COMPACTION_JOB_QUEUE_URL,
                        statusStore, "system-test",
                        PollWithRetries.intervalAndMaxPolls(5000, 12))
                .pollUntilFinished();
    }
}
