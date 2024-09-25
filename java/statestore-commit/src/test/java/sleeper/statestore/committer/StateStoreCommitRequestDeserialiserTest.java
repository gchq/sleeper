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

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequestSerDe;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequestSerDe;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequestSerDe;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3SerDe;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequestTestHelper.requestToAssignFilesToJobs;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStoreCommitRequestDeserialiserTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final List<TableProperties> tables = new ArrayList<>();
    private final Map<String, String> dataBucketObjectByKey = new HashMap<>();

    @Test
    void shouldDeserialiseCompactionJobCommitRequest() {
        // Given
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(List.of("file1.parquet", "file2.parquet"))
                .outputFile("test-output.parquet")
                .partitionId("test-partition-id")
                .build();
        CompactionJobCommitRequest compactionJobCommitRequest = new CompactionJobCommitRequest(
                job, "test-task", "test-job-run",
                new RecordsProcessedSummary(
                        new RecordsProcessed(120, 100),
                        Instant.parse("2024-05-01T10:58:00Z"), Duration.ofMinutes(1)));
        String jsonString = new CompactionJobCommitRequestSerDe().toJson(compactionJobCommitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forCompactionJob(compactionJobCommitRequest))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldDeserialiseCompactionJobIdAssignmentCommitRequest() {
        // Given
        CompactionJob job1 = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job-1")
                .inputFiles(List.of("file1.parquet", "file2.parquet"))
                .outputFile("test-output-1.parquet")
                .partitionId("test-partition-id")
                .build();
        CompactionJob job2 = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job-2")
                .inputFiles(List.of("file3.parquet", "file4.parquet"))
                .outputFile("test-output-2.parquet")
                .partitionId("test-partition-id")
                .build();
        CompactionJobIdAssignmentCommitRequest jobIdAssignmentRequest = requestToAssignFilesToJobs(
                List.of(job1, job2), "test-table");
        String jsonString = new CompactionJobIdAssignmentCommitRequestSerDe().toJson(jobIdAssignmentRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forCompactionJobIdAssignment(jobIdAssignmentRequest))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldDeserialiseIngestJobCommitRequest() {
        // Given
        IngestJob job = IngestJob.builder()
                .id("test-job-id")
                .files(List.of("file1.parquet", "file2.parquet"))
                .tableId("test-table-id")
                .tableName("test-table-name")
                .build();
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
        IngestAddFilesCommitRequest ingestJobCommitRequest = IngestAddFilesCommitRequest.builder()
                .ingestJob(job)
                .taskId("test-task")
                .jobRunId("test-job-run")
                .fileReferences(List.of(file1, file2))
                .writtenTime(Instant.parse("2024-06-20T15:57:01Z"))
                .build();
        String jsonString = new IngestAddFilesCommitRequestSerDe().toJson(ingestJobCommitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forIngestAddFiles(ingestJobCommitRequest))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table-id");
    }

    @Test
    void shouldDeserialiseIngestCommitRequestWithNoJob() {
        // Given
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
        IngestAddFilesCommitRequest ingestJobCommitRequest = IngestAddFilesCommitRequest.builder()
                .tableId("test-table")
                .fileReferences(List.of(file1, file2))
                .build();
        String jsonString = new IngestAddFilesCommitRequestSerDe().toJson(ingestJobCommitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forIngestAddFiles(ingestJobCommitRequest))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldDeserialiseSplitPartitionCommitRequest() {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "aaa")
                .buildTree();
        SplitPartitionCommitRequest splitPartitionCommitRequest = new SplitPartitionCommitRequest(
                "test-table", partitionTree.getRootPartition(),
                partitionTree.getPartition("left"), partitionTree.getPartition("right"));
        createTable("test-table", schema);

        String jsonString = new SplitPartitionCommitRequestSerDe(schema).toJson(splitPartitionCommitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forSplitPartition(splitPartitionCommitRequest))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldDeserialiseGarbageCollectionCommitRequest() {
        // Given
        createTable("test-table", schemaWithKey("key"));
        GarbageCollectionCommitRequest request = new GarbageCollectionCommitRequest(
                "test-table", List.of("file1.parquet", "file2.parquet"));

        String jsonString = new GarbageCollectionCommitRequestSerDe().toJson(request);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forGarbageCollection(request))
                .extracting(StateStoreCommitRequest::getTableId).isEqualTo("test-table");
    }

    @Test
    void shouldDeserialiseCommitRequestInS3() {
        // Given
        FileReference file = FileReference.builder()
                .filename("file.parquet")
                .partitionId("root")
                .numberOfRecords(100L)
                .onlyContainsDataForThisPartition(true)
                .build();
        IngestAddFilesCommitRequest requestInBucket = IngestAddFilesCommitRequest.builder()
                .tableId("test-table")
                .fileReferences(List.of(file))
                .build();
        String s3Key = StateStoreCommitRequestInS3.createFileS3Key("test-table", "test-file");
        dataBucketObjectByKey.put(s3Key, new IngestAddFilesCommitRequestSerDe().toJson(requestInBucket));
        StateStoreCommitRequestInS3 commitRequest = new StateStoreCommitRequestInS3(s3Key);
        String jsonString = new StateStoreCommitRequestInS3SerDe().toJson(commitRequest);

        // When / Then
        assertThat(deserialiser().fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forIngestAddFiles(requestInBucket))
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

    private void createTable(String tableId, Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tables.add(tableProperties);
    }

    private StateStoreCommitRequestDeserialiser deserialiser() {
        return new StateStoreCommitRequestDeserialiser(new FixedTablePropertiesProvider(tables), dataBucketObjectByKey::get);
    }
}
