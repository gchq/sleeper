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
package sleeper.systemtest.drivers.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetEventSourceMappingResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import sleeper.core.util.PollWithRetries;
import sleeper.core.util.SplitIntoBatches;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_EVENT_SOURCE_ID;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

public class AwsStateStoreCommitterDriver implements StateStoreCommitterDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsStateStoreCommitterDriver.class);

    private final SystemTestInstanceContext instance;
    private final SqsClient sqs;
    private final LambdaClient lambda;

    public AwsStateStoreCommitterDriver(SystemTestInstanceContext instance, SqsClient sqs, LambdaClient lambda) {
        this.instance = instance;
        this.sqs = sqs;
        this.lambda = lambda;
    }

    @Override
    public void sendCommitMessagesInParallelBatches(Stream<StateStoreCommitMessage> messages) {
        SplitIntoBatches.streamBatchesOf(10, messages).parallel().forEach(this::sendMessageBatch);
    }

    @Override
    public void sendCommitMessagesInSequentialBatches(Stream<StateStoreCommitMessage> messages) {
        SplitIntoBatches.streamBatchesOf(10, messages).sequential().forEach(this::sendMessageBatch);
    }

    private void sendMessageBatch(List<StateStoreCommitMessage> batch) {
        sqs.sendMessageBatch(request -> request
                .queueUrl(instance.getInstanceProperties().get(STATESTORE_COMMITTER_QUEUE_URL))
                .entries(batch.stream()
                        .map(message -> SendMessageBatchRequestEntry.builder()
                                .messageDeduplicationId(UUID.randomUUID().toString())
                                .id(UUID.randomUUID().toString())
                                .messageGroupId(message.getTableId())
                                .messageBody(message.getBody())
                                .build())
                        .collect(toUnmodifiableList())));
    }

    @Override
    public void pauseReceivingMessages() {
        GetEventSourceMappingResponse mapping = getEventSourceMapping();
        LOGGER.info("Disabling event source for state store committer: {}", mapping.functionArn());
        setEventSourceEnabledWaitForState(mapping, false, "Disabled");
    }

    @Override
    public void resumeReceivingMessages() {
        GetEventSourceMappingResponse mapping = getEventSourceMapping();
        LOGGER.info("Enabling event source for state store committer: {}", mapping.functionArn());
        setEventSourceEnabledWaitForState(mapping, true, "Enabled");
    }

    private void setEventSourceEnabledWaitForState(GetEventSourceMappingResponse mapping, boolean enabled, String state) {
        lambda.updateEventSourceMapping(builder -> builder
                .uuid(mapping.uuid())
                .functionName(mapping.functionArn())
                .batchSize(mapping.batchSize())
                .enabled(enabled));
        try {
            PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .pollUntil("event source has expected state",
                            () -> {
                                GetEventSourceMappingResponse response = getEventSourceMapping();
                                LOGGER.info("Found event source state: {}", response.state());
                                return Objects.equals(state, response.state());
                            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private GetEventSourceMappingResponse getEventSourceMapping() {
        String uuid = instance.getInstanceProperties().get(STATESTORE_COMMITTER_EVENT_SOURCE_ID);
        return lambda.getEventSourceMapping(builder -> builder.uuid(uuid));
    }
}
