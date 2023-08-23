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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.sqs.AmazonSQS;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.util.InvokeSystemTestLambda;

public class SplittingCompactionDriver {

    private final SleeperInstanceContext instance;
    private final LambdaClient lambdaClient;
    private final AmazonSQS sqsClient;
    private final AmazonDynamoDB dynamoDBClient;

    public SplittingCompactionDriver(SleeperInstanceContext instance,
                                     LambdaClient lambdaClient, AmazonSQS sqsClient, AmazonDynamoDB dynamoDBClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
        this.sqsClient = sqsClient;
        this.dynamoDBClient = dynamoDBClient;
    }

    public void runSplittingCompaction() throws InterruptedException {
        InstanceProperties properties = instance.getInstanceProperties();
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, properties);
        WaitForCurrentSplitAddingMissingJobs.from(
                        InvokeSystemTestLambda.client(lambdaClient, properties),
                        sqsClient, store, properties, instance.getTableName())
                .checkIfSplittingCompactionNeededAndWait();
    }
}
