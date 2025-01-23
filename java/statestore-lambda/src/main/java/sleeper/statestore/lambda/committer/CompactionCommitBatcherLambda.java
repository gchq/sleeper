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
package sleeper.statestore.lambda.committer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.commit.CompactionCommitBatcher;
import sleeper.compaction.core.job.commit.CompactionCommitRequest;
import sleeper.compaction.core.job.commit.CompactionCommitRequestSerDe;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDeProvider;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;

import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that combines multiple compaction commits into a single transaction per Sleeper table.
 */
public class CompactionCommitBatcherLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {

    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionCommitBatcherLambda.class);
    private final CompactionCommitRequestSerDe serDe = new CompactionCommitRequestSerDe();
    private final CompactionCommitBatcher batcher;

    public CompactionCommitBatcherLambda() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        String bucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, bucketName);
        this.batcher = createBatcher(instanceProperties, sqsClient, s3Client);
    }

    public CompactionCommitBatcherLambda(CompactionCommitBatcher batcher) {
        this.batcher = batcher;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        List<BatchItemFailure> failures = new ArrayList<>();
        List<CompactionCommitRequest> requests = event.getRecords().stream()
                .map(message -> readMessageTrackingFailure(message, failures))
                .toList();
        batcher.sendBatch(requests);
        return new SQSBatchResponse(failures);
    }

    private CompactionCommitRequest readMessageTrackingFailure(SQSMessage message, List<BatchItemFailure> failures) {
        return serDe.fromJsonWithCallbackOnFail(message.getBody(),
                () -> failures.add(new BatchItemFailure(message.getMessageId())));
    }

    /**
     * Creates the batcher used to send requests to the state store committer queue via SQS.
     *
     * @param  instanceProperties the instance properties
     * @param  sqsClient          the SQS client
     * @param  s3Client           the S3 client
     * @return                    the batcher
     */
    public static CompactionCommitBatcher createBatcher(
            InstanceProperties instanceProperties, AmazonSQS sqsClient, AmazonS3 s3Client) {
        return new CompactionCommitBatcher(new SqsFifoStateStoreCommitRequestSender(
                instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.forFileTransactions()));
    }

}
