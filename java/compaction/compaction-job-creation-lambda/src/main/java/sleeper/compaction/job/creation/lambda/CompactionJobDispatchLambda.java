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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.compaction.job.creationv2.AwsCompactionJobDispatcher;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Sends compaction jobs in batches from the pending jobs queue, running in AWS Lambda.
 * The jobs are created by {@link CreateCompactionJobsLambda}, then are sent in batches in this class.
 * This lambda also handles waiting for input files to be assigned to the jobs,
 * when that is done asynchronously. Runs batches with {@link CompactionJobDispatcher}.
 */
public class CompactionJobDispatchLambda implements RequestHandler<SQSEvent, Void> {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobDispatchLambda.class);

    private final CompactionJobDispatcher dispatcher;
    private final CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();

    public CompactionJobDispatchLambda() {
        S3Client s3 = S3Client.create();
        DynamoDbClient dynamoDB = DynamoDbClient.create();
        SqsClient sqs = SqsClient.create();
        S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(S3AsyncClient.create()).build();
        String configBucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3, configBucket);
        dispatcher = AwsCompactionJobDispatcher.from(s3, dynamoDB, sqs, s3TransferManager, instanceProperties, Instant::now);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        event.getRecords().forEach(message -> dispatcher.dispatch(serDe.fromJson(message.getBody())));
        return null;
    }
}
