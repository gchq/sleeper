/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.compaction.core.job.commit;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.table.TableFilePaths;
import sleeper.core.table.TableIdGenerator;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.ID_MAX_LENGTH;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.testutils.SupplierTestHelper.exampleUUID;
import static sleeper.core.testutils.SupplierTestHelper.numberedUUID;

public class CompactionCommitMessageSerDeTest {

    InstanceProperties instanceProperties = createTestInstancePropertiesWithId("I".repeat(ID_MAX_LENGTH));
    Schema schema = schemaWithKey("key");
    String tableId = TableIdGenerator.fromRandomSeed(0).generateString();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema, tableId);
    TableFilePaths filePaths = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
    CompactionCommitMessageSerDe serDe = new CompactionCommitMessageSerDe();

    @Test
    void shouldSerialiseCompactionCommitRequest() {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree();
        ReplaceFileReferencesRequest filesRequest = ReplaceFileReferencesRequest.builder()
                .jobId("test-job")
                .taskId("test-task")
                .jobRunId("test-run")
                .inputFiles(List.of("test.parquet"))
                .newReference(FileReferenceFactory.from(partitions).rootFile("output.parquet", 200))
                .build();
        CompactionCommitMessage message = new CompactionCommitMessage("test-table", filesRequest);
        Runnable callbackOnFail = () -> {
        };

        // When
        String json = serDe.toJson(message);
        CompactionCommitMessageHandle found = serDe.fromJsonWithCallbackOnFail(json, callbackOnFail);

        // Then
        assertThat(found).isEqualTo(new CompactionCommitMessageHandle("test-table", filesRequest, callbackOnFail));
    }

    @Test
    void shouldSerialiseRepresentativeCompactionCommitRequest() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createExampleFilesRequestOfRepresentativeSize();
        CompactionCommitMessage message = new CompactionCommitMessage(tableId, filesRequest);
        Runnable callbackOnFail = () -> {
        };

        // When
        String json = serDe.toJson(message);
        CompactionCommitMessageHandle found = serDe.fromJsonWithCallbackOnFail(json, callbackOnFail);

        // Then
        assertThat(found).isEqualTo(new CompactionCommitMessageHandle(tableId, filesRequest, callbackOnFail));
    }

    @Test
    void shouldGenerateExpectedJson() {
        // Given
        CompactionCommitMessage message = new CompactionCommitMessage(tableId, createExampleFilesRequestOfRepresentativeSize());

        // When / Then
        Approvals.verify(serDe.toJson(message), new Options().forFile()
                .withName("example-compaction-commit", ".json"));
    }

    @Test
    void shouldGenerateExpectedPrettyJson() {
        // Given
        CompactionCommitMessage message = new CompactionCommitMessage(tableId, createExampleFilesRequestOfRepresentativeSize());

        // When / Then
        Approvals.verify(serDe.toJsonPrettyPrint(message), new Options().forFile()
                .withName("example-compaction-commit-pretty", ".json"));
    }

    private ReplaceFileReferencesRequest createExampleFilesRequestOfRepresentativeSize() {
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition(exampleUUID("partn", 0)).buildTree();
        List<String> inputFiles = IntStream.rangeClosed(1, 11)
                .mapToObj(i -> filePaths.constructPartitionParquetFilePath(
                        partitions.getRootPartition(), numberedUUID("file", i)))
                .toList();
        FileReference outputFile = FileReferenceFactory.from(partitions).rootFile(
                filePaths.constructPartitionParquetFilePath(
                        partitions.getRootPartition(), exampleUUID("file", 'N')),
                100_000_000);
        return ReplaceFileReferencesRequest.builder()
                .jobId(exampleUUID("job", 1))
                .taskId(exampleUUID("task", 1))
                .jobRunId(exampleUUID("run", 1))
                .inputFiles(inputFiles)
                .newReference(outputFile)
                .build();
    }

    private static TableProperties createTestTableProperties(InstanceProperties instanceProperties, Schema schema, String tableId) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, "test");
        return tableProperties;
    }

}
