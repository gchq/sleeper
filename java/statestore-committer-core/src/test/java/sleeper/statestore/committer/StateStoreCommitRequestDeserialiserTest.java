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
package sleeper.statestore.committer;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.commit.StateStoreCommitRequestByTransaction;
import sleeper.core.statestore.commit.StateStoreCommitRequestByTransactionSerDe;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3SerDe;
import sleeper.core.statestore.transactionlog.PartitionTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStoreCommitRequestDeserialiserTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final List<TableProperties> tables = new ArrayList<>();
    private final Map<String, String> dataBucketObjectByKey = new HashMap<>();

    @Test
    void shouldDeserialiseIngestJobCommitRequest() {
        // Given
        createTable("test-table", schemaWithKey("key"));
        FileReference file1 = FileReference.builder()
                .filename("file1.parquet")
                .partitionId("root")
                .numberOfRecords(100L)
                .onlyContainsDataForThisPartition(true)
                .build();
        FileReference file2 = FileReference.builder()
                .filename("file2.parquet")
                .partitionId("root")
                .numberOfRecords(200L)
                .onlyContainsDataForThisPartition(true)
                .build();
        AddFilesTransaction transaction = AddFilesTransaction.builder()
                .jobId("test-job")
                .taskId("test-task")
                .jobRunId("test-job-run")
                .writtenTime(Instant.parse("2024-06-20T15:57:01Z"))
                .files(AllReferencesToAFile.newFilesWithReferences(List.of(file1, file2)))
                .build();
        StateStoreCommitRequestByTransaction request = StateStoreCommitRequestByTransaction.create("test-table", transaction);
        String jsonString = requestByTransactionSerDe().toJson(request);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forTransaction(request))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldDeserialiseIngestCommitRequestWithNoJob() {
        // Given
        createTable("test-table", schemaWithKey("key"));
        FileReference file1 = FileReference.builder()
                .filename("file1.parquet")
                .partitionId("root")
                .numberOfRecords(100L)
                .onlyContainsDataForThisPartition(true)
                .build();
        FileReference file2 = FileReference.builder()
                .filename("file2.parquet")
                .partitionId("root")
                .numberOfRecords(200L)
                .onlyContainsDataForThisPartition(true)
                .build();
        AddFilesTransaction transaction = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1, file2)));
        StateStoreCommitRequestByTransaction request = StateStoreCommitRequestByTransaction.create("test-table", transaction);
        String jsonString = requestByTransactionSerDe().toJson(request);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forTransaction(request))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldDeserialiseCommitRequestInS3() {
        // Given
        createTable("test-table", schemaWithKey("key"));
        FileReference file = FileReference.builder()
                .filename("file.parquet")
                .partitionId("root")
                .numberOfRecords(100L)
                .onlyContainsDataForThisPartition(true)
                .build();
        AddFilesTransaction transaction = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file)));
        StateStoreCommitRequestByTransaction requestInBucket = StateStoreCommitRequestByTransaction.create("test-table", transaction);
        String s3Key = StateStoreCommitRequestInS3.createFileS3Key("test-table", "test-file");
        dataBucketObjectByKey.put(s3Key, requestByTransactionSerDe().toJson(requestInBucket));
        StateStoreCommitRequestInS3 commitRequest = new StateStoreCommitRequestInS3(s3Key);
        String jsonString = new StateStoreCommitRequestInS3SerDe().toJson(commitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forTransaction(requestInBucket))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldRefuseReferenceToS3HeldInS3() throws Exception {
        // Given we have a request pointing to itself
        String s3Key = StateStoreCommitRequestInS3.createFileS3Key("test-table", "test-file");
        StateStoreCommitRequestInS3 commitRequest = new StateStoreCommitRequestInS3(s3Key);
        dataBucketObjectByKey.put(s3Key, new StateStoreCommitRequestInS3SerDe().toJson(commitRequest));
        String jsonString = new StateStoreCommitRequestInS3SerDe().toJson(commitRequest);

        // When / Then
        StateStoreCommitRequestDeserialiser deserialiser = deserialiser();
        assertThatThrownBy(() -> deserialiser.fromJson(jsonString))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowExceptionIfCommitRequestTypeInvalid() {
        // Given
        String jsonString = "{\"type\":\"invalid-type\", \"request\":{}}";

        // When / Then
        assertThatThrownBy(() -> deserialiser().fromJson(jsonString))
                .isInstanceOf(CommitRequestValidationException.class);
    }

    @Test
    void shouldDeserialiseTransactionInS3() {
        // Given
        String key = TransactionBodyStore.createObjectKey("test-table", Instant.parse("2025-01-14T15:30:00Z"), "test-transaction");
        StateStoreCommitRequestByTransaction commitRequest = StateStoreCommitRequestByTransaction.create("test-table", key, TransactionType.INITIALISE_PARTITIONS);
        String jsonString = requestByTransactionSerDe().toJson(commitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forTransaction(commitRequest));
    }

    @Test
    void shouldDeserialiseInitialisePartitionsTransactionInRequest() {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        createTable("test-table", schema);
        PartitionTransaction transaction = new InitialisePartitionsTransaction(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 5L)
                .buildList());
        StateStoreCommitRequestByTransaction commitRequest = StateStoreCommitRequestByTransaction.create("test-table", transaction);
        String jsonString = requestByTransactionSerDe().toJson(commitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forTransaction(commitRequest));
    }

    private void createTable(String tableId, Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tables.add(tableProperties);
    }

    private StateStoreCommitRequestDeserialiser deserialiser() {
        return new StateStoreCommitRequestDeserialiser(new FixedTablePropertiesProvider(tables), dataBucketObjectByKey::get);
    }

    private StateStoreCommitRequestByTransactionSerDe requestByTransactionSerDe() {
        return new StateStoreCommitRequestByTransactionSerDe(new FixedTablePropertiesProvider(tables));
    }
}
