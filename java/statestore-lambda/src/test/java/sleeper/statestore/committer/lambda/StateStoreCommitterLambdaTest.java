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
package sleeper.statestore.committer.lambda;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.FilesReportTestHelper;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.statestore.committer.StateStoreCommitRequestDeserialiser;
import sleeper.statestore.committer.StateStoreCommitter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithPartitions;

public class StateStoreCommitterLambdaTest {
    private static final Instant DEFAULT_FILE_UPDATE_TIME = FilesReportTestHelper.DEFAULT_UPDATE_TIME;
    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithPartitions(partitions.getAllPartitions());
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_FILE_UPDATE_TIME);

    @BeforeEach
    void setUp() {
        stateStore.fixFileUpdateTime(DEFAULT_FILE_UPDATE_TIME);
    }

    @Test
    void shouldApplyOneCommit() throws Exception {
        // Given
        FileReference file = fileFactory.rootFile("test.parquet", 100);

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("test-message", file)),
                null);

        // Then
        assertThat(response.getBatchItemFailures()).isEmpty();
        assertThat(stateStore.getFileReferences())
                .containsExactly(file);
    }

    @Test
    void shouldFailSomeCommitsInBatch() throws Exception {
        // Given
        FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
        FileReference duplicate1 = fileFactory.rootFile("file-1.parquet", 200);
        FileReference file2 = fileFactory.rootFile("file-2.parquet", 300);
        FileReference duplicate2 = fileFactory.rootFile("file-2.parquet", 400);

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("message-1", file1),
                addFilesMessage("message-2", duplicate1),
                addFilesMessage("message-3", file2),
                addFilesMessage("message-4", duplicate2)),
                null);

        // Then
        assertThat(response.getBatchItemFailures())
                .extracting(BatchItemFailure::getItemIdentifier)
                .containsExactly("message-2", "message-4");
        assertThat(stateStore.getFileReferences())
                .containsExactly(file1, file2);
    }

    private StateStoreCommitterLambda lambda() {
        return new StateStoreCommitterLambda(deserialiser(), committer(), PollWithRetries.immediateRetries(10));
    }

    private StateStoreCommitRequestDeserialiser deserialiser() {
        return new StateStoreCommitRequestDeserialiser(
                new FixedTablePropertiesProvider(tableProperties),
                s3Key -> {
                    throw new IllegalArgumentException("Unexpected request to load from data bucket key " + s3Key);
                });
    }

    private StateStoreCommitter committer() {
        return new StateStoreCommitter(CompactionJobStatusStore.NONE, IngestJobStatusStore.NONE,
                Map.of(tableProperties.get(TABLE_ID), stateStore)::get,
                Instant::now);
    }

    private SQSEvent event(SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private SQSMessage addFilesMessage(String messageId, FileReference... files) {
        IngestAddFilesCommitRequest request = IngestAddFilesCommitRequest.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .fileReferences(List.of(files))
                .build();
        SQSMessage message = new SQSMessage();
        message.setMessageId(messageId);
        message.setBody(new IngestAddFilesCommitRequestSerDe().toJson(request));
        return message;
    }

}
