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

import com.amazonaws.services.s3.AmazonS3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequest;
import sleeper.statestore.committer.StateStoreCommitRequest;
import sleeper.statestore.committer.StateStoreCommitRequestDeserialiser;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.MAIN;

@LocalStackDslTest
public class AwsStateStoreCommitterDriverIT {

    private SqsClient sqs;
    private AmazonS3 s3;
    private SystemTestInstanceContext instance;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, SystemTestContext context, LocalStackSystemTestDrivers drivers) {
        sleeper.connectToInstance(MAIN);
        sqs = drivers.clients().getSqsV2();
        s3 = drivers.clients().getS3();
        instance = context.instance();
    }

    @Test
    void shouldSendCommitToSqsQueue(SleeperSystemTest sleeper) {
        // When
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        FileReference file = FileReferenceFactory.from(partitions).rootFile("file.parquet", 123);
        sleeper.partitioning().setPartitions(partitions);
        sleeper.stateStore().fakeCommits().send(StateStoreCommitMessage.addFile(file));

        // Then
        String tableId = sleeper.tableProperties().get(TABLE_ID);
        assertThat(receiveCommitRequests(sleeper))
                .extracting(this::getMessageGroupId, this::readCommitRequest)
                .containsExactly(tuple(tableId,
                        StateStoreCommitRequest.forIngestAddFiles(IngestAddFilesCommitRequest.builder()
                                .tableId(tableId)
                                .fileReferences(List.of(file))
                                .build())));
    }

    @Test
    void shouldSendMoreCommitsThanBatchSize(SleeperSystemTest sleeper) {
        // When
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        List<FileReference> files = IntStream.rangeClosed(1, 11)
                .mapToObj(i -> fileFactory.rootFile("file-" + i + ".parquet", i))
                .collect(toUnmodifiableList());
        sleeper.partitioning().setPartitions(partitions);
        sleeper.stateStore().fakeCommits().sendBatched(files.stream().map(StateStoreCommitMessage::addFile));

        // Then
        String tableId = sleeper.tableProperties().get(TABLE_ID);
        assertThat(receiveCommitRequestsForBatches(sleeper, 2))
                .extracting(this::getMessageGroupId, this::readCommitRequest)
                .containsExactlyInAnyOrderElementsOf(files.stream().map(file -> tuple(tableId,
                        StateStoreCommitRequest.forIngestAddFiles(IngestAddFilesCommitRequest.builder()
                                .tableId(tableId)
                                .fileReferences(List.of(file))
                                .build())))
                        .collect(toUnmodifiableList()));
    }

    private List<Message> receiveCommitRequests(SleeperSystemTest sleeper) {
        String queueUrl = sleeper.instanceProperties().get(STATESTORE_COMMITTER_QUEUE_URL);
        List<Message> messages = sqs.receiveMessage(request -> request
                .queueUrl(queueUrl)
                .messageSystemAttributeNames(MessageSystemAttributeName.MESSAGE_GROUP_ID)
                .waitTimeSeconds(2)
                .visibilityTimeout(60)
                .maxNumberOfMessages(10))
                .messages();
        sqs.deleteMessageBatch(request -> request
                .queueUrl(queueUrl)
                .entries(messages.stream()
                        .map(message -> DeleteMessageBatchRequestEntry.builder()
                                .id(message.messageId())
                                .receiptHandle(message.receiptHandle())
                                .build())
                        .collect(toUnmodifiableList())));
        return messages;
    }

    private List<Message> receiveCommitRequestsForBatches(SleeperSystemTest sleeper, int batches) {
        List<Message> allMessages = new ArrayList<>();
        for (int i = 0; i < batches; i++) {
            List<Message> messages = receiveCommitRequests(sleeper);
            if (messages.isEmpty()) {
                throw new IllegalStateException("Found no messages in expected batch " + (i + 1) + " of " + batches);
            } else {
                allMessages.addAll(messages);
            }
        }
        return allMessages;
    }

    private String getMessageGroupId(Message message) {
        return message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
    }

    private StateStoreCommitRequest readCommitRequest(Message message) {
        return new StateStoreCommitRequestDeserialiser(instance.getTablePropertiesProvider(),
                key -> s3.getObjectAsString(instance.getInstanceProperties().get(DATA_BUCKET), key))
                .fromJson(message.body());
    }

}
