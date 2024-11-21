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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.hadoop.conf.Configuration;

import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReadBatch;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.ReturnRequestToPendingQueue;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendJob;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class CompactionJobDispatchLambda implements RequestHandler<SQSEvent, Void> {

    private final CompactionJobDispatcher dispatcher;
    private final CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();

    private CompactionJobDispatchLambda() {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String configBucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3, configBucket);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        dispatcher = dispatcher(s3, dynamoDB, sqs, conf, instanceProperties, Instant::now);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        event.getRecords().forEach(message -> dispatcher.dispatch(serDe.fromJson(message.getBody())));
        return null;
    }

    public static CompactionJobDispatcher dispatcher(
            AmazonS3 s3, AmazonDynamoDB dynamoDB, AmazonSQS sqs, Configuration conf, InstanceProperties instanceProperties, Supplier<Instant> timeSupplier) {
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe();
        return new CompactionJobDispatcher(instanceProperties,
                S3TableProperties.createProvider(instanceProperties, s3, dynamoDB),
                StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB, conf),
                readBatch(s3, compactionJobSerDe),
                sendJob(instanceProperties, sqs, compactionJobSerDe),
                returnToQueue(instanceProperties, sqs), timeSupplier);
    }

    private static SendJob sendJob(InstanceProperties instanceProperties, AmazonSQS sqs, CompactionJobSerDe compactionJobSerDe) {
        return compactionJob -> sqs.sendMessage(
                instanceProperties.get(COMPACTION_JOB_QUEUE_URL),
                compactionJobSerDe.toJson(compactionJob));
    }

    private static ReadBatch readBatch(AmazonS3 s3, CompactionJobSerDe compactionJobSerDe) {
        return (bucketName, key) -> compactionJobSerDe.batchFromJson(s3.getObjectAsString(bucketName, key));
    }

    private static ReturnRequestToPendingQueue returnToQueue(InstanceProperties instanceProperties, AmazonSQS sqs) {
        CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
        return (request, delaySeconds) -> sqs.sendMessage(new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL))
                .withMessageBody(serDe.toJson(request))
                .withDelaySeconds(delaySeconds));
    }
}
