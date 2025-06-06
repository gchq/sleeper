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
package sleeper.clients.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.compaction.job.creation.AwsCompactionJobDispatcher;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactoryException;

import java.io.IOException;
import java.time.Instant;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;

public class DispatchCompactionJobsClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(DispatchCompactionJobsClient.class);

    private DispatchCompactionJobsClient() {
    }

    public static void main(String[] args) throws ObjectFactoryException, IOException {
        if (args.length < 1) {
            System.out.println("Usage: <instance-id>");
            return;
        }
        String instanceId = args[0];
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            CompactionJobDispatcher dispatcher = AwsCompactionJobDispatcher.from(s3Client, dynamoClient, sqsClient, instanceProperties, Instant::now);
            CompactionJobDispatchRequestSerDe requestSerDe = new CompactionJobDispatchRequestSerDe();

            ReceiveMessageResponse response = sqsClient.receiveMessage(request -> request.queueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL)));
            for (Message message : response.messages()) {
                CompactionJobDispatchRequest request = requestSerDe.fromJson(message.body());
                dispatcher.dispatch(request);
            }
        }
    }
}
