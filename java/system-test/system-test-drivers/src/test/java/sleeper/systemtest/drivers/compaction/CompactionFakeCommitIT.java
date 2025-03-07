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
package sleeper.systemtest.drivers.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.compaction.core.job.commit.CompactionCommitMessageHandle;
import sleeper.compaction.core.job.commit.CompactionCommitMessageSerDe;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.drivers.util.AwsDrainSqsQueue;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.compaction.StreamFakeCompactions;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@LocalStackDslTest
public class CompactionFakeCommitIT {
    PartitionTree partitions = new PartitionsBuilder(DEFAULT_SCHEMA).singlePartition("root").buildTree();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
    SqsClient sqsClient;
    CompactionCommitMessageSerDe serDe = new CompactionCommitMessageSerDe();
    String compactionCommitQueueUrl;
    String tableId;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, LocalStackSystemTestDrivers drivers) throws Exception {
        sleeper.connectToInstance(LOCALSTACK_MAIN);
        sleeper.partitioning().setPartitions(partitions);
        sqsClient = drivers.clients().getSqsV2();
        compactionCommitQueueUrl = sqsClient.createQueue(builder -> builder.queueName(UUID.randomUUID().toString())).queueUrl();
        sleeper.instanceProperties().set(COMPACTION_COMMIT_QUEUE_URL, compactionCommitQueueUrl);
        tableId = sleeper.tableProperties().get(TABLE_ID);
    }

    @Test
    void shouldFakeCompactionCommits(SleeperSystemTest sleeper) throws Exception {
        // Given
        StreamFakeCompactions compactions = StreamFakeCompactions.builder()
                .numCompactions(100)
                .generateInputFiles(i -> List.of(fileFactory.rootFile("input-" + i + ".parquet", 100)))
                .generateJobId(i -> "job-" + i)
                .generateOutputFile(i -> fileFactory.rootFile("output-" + i + ".parquet", 100))
                .build();
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            compactions.streamAddFiles().forEach(update(store)::addTransaction);
            compactions.streamAssignJobIds().forEach(update(store)::addTransaction);
        });

        // When
        sleeper.compaction()
                .sendFakeCommits(compactions);

        // Then
        assertThat(drainCommitQueue()).isEqualTo(
                compactions.streamCommitRequests()
                        .map(this::handle)
                        .collect(toSet()));
    }

    private Set<CompactionCommitMessageHandle> drainCommitQueue() {
        return AwsDrainSqsQueue.forLocalStackTests(sqsClient)
                .drain(compactionCommitQueueUrl)
                .map(this::readCommitMessage)
                .collect(toSet());
    }

    private CompactionCommitMessageHandle readCommitMessage(Message message) {
        return serDe.fromJsonWithCallbackOnFail(message.body(), null);
    }

    private CompactionCommitMessageHandle handle(ReplaceFileReferencesRequest request) {
        return new CompactionCommitMessageHandle(tableId, request, null);
    }
}
