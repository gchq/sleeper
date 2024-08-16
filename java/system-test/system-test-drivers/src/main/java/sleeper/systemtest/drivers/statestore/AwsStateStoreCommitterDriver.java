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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.EventSourceMappingConfiguration;
import software.amazon.awssdk.services.lambda.model.GetEventSourceMappingResponse;

import sleeper.core.util.PollWithRetries;
import sleeper.core.util.SplitIntoBatches;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

public class AwsStateStoreCommitterDriver implements StateStoreCommitterDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsStateStoreCommitterDriver.class);

    private final SystemTestInstanceContext instance;
    private final AmazonSQS sqs;
    private final LambdaClient lambda;

    public AwsStateStoreCommitterDriver(SystemTestInstanceContext instance, AmazonSQS sqs, LambdaClient lambda) {
        this.instance = instance;
        this.sqs = sqs;
        this.lambda = lambda;
    }

    @Override
    public void sendCommitMessages(Stream<StateStoreCommitMessage> messages) {
        SplitIntoBatches.inParallelBatchesOf(10, messages, this::sendMessageBatch);
    }

    private void sendMessageBatch(List<StateStoreCommitMessage> batch) {
        sqs.sendMessageBatch(new SendMessageBatchRequest()
                .withQueueUrl(instance.getInstanceProperties().get(STATESTORE_COMMITTER_QUEUE_URL))
                .withEntries(batch.stream()
                        .map(message -> new SendMessageBatchRequestEntry()
                                .withMessageDeduplicationId(UUID.randomUUID().toString())
                                .withId(UUID.randomUUID().toString())
                                .withMessageGroupId(message.getTableId())
                                .withMessageBody(message.getBody()))
                        .collect(toUnmodifiableList())));
    }

    @Override
    public void pauseReceivingMessages() {
        EventSourceMappingConfiguration mapping = findEventSourceMapping();
        LOGGER.info("Disabling event source for state store committer: {}", mapping.functionArn());
        setEventSourceEnabledWaitForState(mapping, false, "Disabled");
    }

    @Override
    public void resumeReceivingMessages() {
        EventSourceMappingConfiguration mapping = findEventSourceMapping();
        LOGGER.info("Enabling event source for state store committer: {}", mapping.functionArn());
        setEventSourceEnabledWaitForState(mapping, true, "Enabled");
    }

    private void setEventSourceEnabledWaitForState(EventSourceMappingConfiguration mapping, boolean enabled, String state) {
        lambda.updateEventSourceMapping(builder -> builder
                .uuid(mapping.uuid())
                .functionName(mapping.functionArn())
                .batchSize(mapping.batchSize())
                .enabled(enabled));
        try {
            PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .pollUntil("event source has expected state",
                            () -> eventSourceHasState(mapping.uuid(), state));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private EventSourceMappingConfiguration findEventSourceMapping() {
        String queueArn = instance.getInstanceProperties().get(STATESTORE_COMMITTER_QUEUE_ARN);
        List<EventSourceMappingConfiguration> mappings = lambda
                .listEventSourceMappings(builder -> builder.eventSourceArn(queueArn))
                .eventSourceMappings();
        if (mappings.size() != 1) {
            throw new IllegalStateException("Expected 1 mapping for committer queue, found: " + mappings);
        }
        return mappings.get(0);
    }

    private boolean eventSourceHasState(String uuid, String state) {
        GetEventSourceMappingResponse response = lambda.getEventSourceMapping(builder -> builder.uuid(uuid));
        LOGGER.info("Found event source state: {}", response.state());
        return false;
    }
}
