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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.util.InvokeSystemTestLambda;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_LAMBDA_FUNCTION;
import static sleeper.systemtest.util.InvokeSystemTestLambda.createSystemTestLambdaClient;

public class SplitPartitionsUntilNoMoreSplits {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitPartitionsUntilNoMoreSplits.class);

    private SplitPartitionsUntilNoMoreSplits() {
    }

    public static void main(String[] args) throws IOException, InterruptedException, StateStoreException {
        if (args.length != 2) {
            System.out.println("Usage: <instance id> <table name>");
            return;
        }

        String instanceId = args[0];
        String tableName = args[1];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        CompactionJobStatusStore store = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, systemTestProperties);

        TableProperties tableProperties = new TableProperties(systemTestProperties);
        tableProperties.loadFromS3(s3Client, tableName);
        StateStore stateStore = new StateStoreProvider(dynamoDBClient, systemTestProperties)
                .getStateStore(tableProperties);

        try (LambdaClient lambdaClient = createSystemTestLambdaClient()) {
            InvokeSystemTestLambda.Client lambda = InvokeSystemTestLambda.client(lambdaClient, systemTestProperties);
            WaitForCurrentSplitAddingMissingJobs applySplit = WaitForCurrentSplitAddingMissingJobs.from(
                    lambda, sqsClient, store, systemTestProperties, tableName);

            int splittingRound = 1;
            do {
                WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                        .forCurrentPartitionsNeedingSplitting(tableProperties, stateStore);
                LOGGER.info("Splitting partitions, round {}", splittingRound);
                splittingRound++;
                lambda.invokeLambda(PARTITION_SPLITTING_LAMBDA_FUNCTION);
                waitForPartitionSplitting.pollUntilFinished(stateStore);
            } while (applySplit.checkIfSplittingCompactionNeededAndWait()); // Repeat until no more splitting is needed
        }
    }
}
