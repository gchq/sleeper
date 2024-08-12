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

import sleeper.core.util.SplitIntoBatches;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

public class AwsStateStoreCommitterDriver implements StateStoreCommitterDriver {

    private final SystemTestInstanceContext instance;
    private final AmazonSQS sqs;

    public AwsStateStoreCommitterDriver(SystemTestInstanceContext instance, AmazonSQS sqs) {
        this.instance = instance;
        this.sqs = sqs;
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

}
