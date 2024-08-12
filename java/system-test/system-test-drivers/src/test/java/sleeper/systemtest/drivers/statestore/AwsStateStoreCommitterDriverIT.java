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
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.commit.StateStoreCommitRequest;
import sleeper.commit.StateStoreCommitRequestDeserialiser;
import sleeper.commit.StateStoreCommitter.LoadS3ObjectFromDataBucket;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.MAIN;

@LocalStackDslTest
public class AwsStateStoreCommitterDriverIT {

    private AmazonSQS sqs;
    private SystemTestInstanceContext instance;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, SystemTestContext context, LocalStackSystemTestDrivers drivers) {
        sleeper.connectToInstance(MAIN);
        sqs = drivers.clients().getSqs();
        instance = context.instance();
    }

    @Test
    void shouldSendCommitToSqsQueue(SleeperSystemTest sleeper) {
        // When
        PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
        FileReference file = FileReferenceFactory.from(partitions).rootFile("file.parquet", 123);
        sleeper.partitioning().setPartitions(partitions);
        sleeper.stateStore().fakeCommits().send(factory -> factory.addFile(file));

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
        sleeper.stateStore().fakeCommits().sendBatched(factory -> files.stream().map(file -> factory.addFile(file)));

        // Then
        String tableId = sleeper.tableProperties().get(TABLE_ID);
        assertThat(receiveCommitRequestsForBatches(sleeper, 2))
                .extracting(this::getMessageGroupId, this::readCommitRequest)
                .containsExactlyElementsOf(files.stream().map(file -> tuple(tableId,
                        StateStoreCommitRequest.forIngestAddFiles(IngestAddFilesCommitRequest.builder()
                                .tableId(tableId)
                                .fileReferences(List.of(file))
                                .build())))
                        .collect(toUnmodifiableList()));
    }

    private List<Message> receiveCommitRequests(SleeperSystemTest sleeper) {
        return sqs.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(sleeper.instanceProperties().get(STATESTORE_COMMITTER_QUEUE_URL))
                .withAttributeNames("MessageGroupId")
                .withWaitTimeSeconds(2)
                .withVisibilityTimeout(60)
                .withMaxNumberOfMessages(10))
                .getMessages();
    }

    private List<Message> receiveCommitRequestsForBatches(SleeperSystemTest sleeper, int batches) {
        List<Message> allMessages = new ArrayList<>();
        for (int i = 0; i < batches; i++) {
            List<Message> messages = receiveCommitRequests(sleeper);
            if (messages.isEmpty()) {
                break;
            } else {
                allMessages.addAll(messages);
            }
        }
        return allMessages;
    }

    private String getMessageGroupId(Message message) {
        return message.getAttributes().get("MessageGroupId");
    }

    private StateStoreCommitRequest readCommitRequest(Message message) {
        LoadS3ObjectFromDataBucket noLoadFromS3 = key -> {
            throw new UnsupportedOperationException("Did not expect message held in S3");
        };
        return new StateStoreCommitRequestDeserialiser(instance.getTablePropertiesProvider(), noLoadFromS3)
                .fromJson(message.getBody());
    }

}
