/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.compaction;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.RequeueCompactionJobsFromStateStore;
import sleeper.compaction.job.creation.AwsCompactionJobDispatcher;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Command line client to recreate compaction jobs for all files in a Sleeper table that are assigned to a compaction
 * job, and send them to the compaction job queue. This can be used to recover when files are assigned to compaction
 * jobs that did not succeed.
 */
public class RequeueCompactionJobsFromStateStoreClient {

    private RequeueCompactionJobsFromStateStoreClient() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
            TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient)
                    .getByName(tableName);
            StateStore stateStore = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient)
                    .getStateStore(tableProperties);
            RequeueCompactionJobsFromStateStore requeue = new RequeueCompactionJobsFromStateStore(instanceProperties,
                    AwsCompactionJobDispatcher.sendJobs(instanceProperties, sqsClient, new CompactionJobSerDe()),
                    CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties));
            List<CompactionJob> jobs = requeue.requeue(tableProperties, stateStore);
            System.out.println("Requeued " + jobs.size() + " compaction jobs for table " + tableName);
        }
    }
}
