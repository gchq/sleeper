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
package sleeper.core.statestore.transactionlog.transactions;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.FileReferenceTransactionGeneric;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.StateStoreTransactionGeneric;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;
import static sleeper.core.statestore.SplitFileReferenceRequest.splitFileToChildPartitions;

public class TransactionSerDeTest {

    private static void whenSerDeThenMatchAndVerify(Schema schema, StateStoreTransactionGeneric<?> transaction) {
        // When
        TransactionSerDe serDe = new TransactionSerDe(schema);
        TransactionType type = TransactionType.getType(transaction);
        String json = serDe.toJsonPrettyPrint(transaction);

        // Then
        assertThat(serDe.toTransaction(type, json))
                .isEqualTo(transaction);
        Approvals.verify(json);
    }

    @Test
    void shouldSerDeAddFiles() {
        // Given
        Schema schema = schemaWithKey("key");
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        Instant updateTime = Instant.parse("2024-03-26T09:43:01Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, updateTime);
        FileReferenceTransactionGeneric transaction = new AddFilesTransaction(
                AllReferencesToAFile.newFilesWithReferences(Stream.of(
                        fileFactory.rootFile("file1.parquet", 100),
                        fileFactory.rootFile("file2.parquet", 200)),
                        updateTime)
                        .collect(toUnmodifiableList()),
                updateTime);

        // When / Then
        whenSerDeThenMatchAndVerify(schema, transaction);
    }

    @Test
    void shouldSerDeAddSplitFile() {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "p")
                .buildTree();
        Instant updateTime = Instant.parse("2024-03-26T09:43:01Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, updateTime);
        FileReference file = fileFactory.rootFile("file.parquet", 200);
        referenceForChildPartition(file, "L");
        FileReferenceTransactionGeneric transaction = new AddFilesTransaction(
                AllReferencesToAFile.newFilesWithReferences(Stream.of(
                        referenceForChildPartition(file, "L"),
                        referenceForChildPartition(file, "R")),
                        updateTime).collect(toUnmodifiableList()),
                updateTime);

        // When / Then
        whenSerDeThenMatchAndVerify(schema, transaction);
    }

    @Test
    void shouldSerDeAssignJobIds() {
        // Given
        FileReferenceTransactionGeneric transaction = new AssignJobIdsTransaction(List.of(
                assignJobOnPartitionToFiles("job1", "root",
                        List.of("file1.parquet", "file2.parquet")),
                assignJobOnPartitionToFiles("job2", "L",
                        List.of("file3.parquet", "file4.parquet"))),
                Instant.parse("2024-03-26T09:44:01Z"));

        // When / Then
        whenSerDeThenMatchAndVerify(schemaWithKey("key"), transaction);
    }

    @Test
    void shouldSerDeClearFiles() {
        // Given
        FileReferenceTransactionGeneric transaction = new ClearFilesTransaction();

        // When / Then
        whenSerDeThenMatchAndVerify(schemaWithKey("key"), transaction);
    }

    @Test
    void shouldSerDeDeleteFiles() {
        // Given
        FileReferenceTransactionGeneric transaction = new DeleteFilesTransaction(List.of("file1.parquet", "file2.parquet"));

        // When / Then
        whenSerDeThenMatchAndVerify(schemaWithKey("key"), transaction);
    }

    @Test
    void shouldSerDeInitialisePartitions() {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        StateStoreTransaction transaction = new InitialisePartitionsTransaction(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "p")
                .splitToNewChildren("L", "LL", "LR", "g")
                .splitToNewChildren("R", "RL", "RR", "u")
                .buildList());

        // When / Then
        whenSerDeThenMatchAndVerify(schema, transaction);
    }

    @Test
    void shouldSerDeReplaceFileReferences() throws Exception {
        // Given
        Schema schema = schemaWithKey("key");
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        Instant updateTime = Instant.parse("2023-03-26T10:05:01Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, updateTime);
        FileReferenceTransactionGeneric transaction = new ReplaceFileReferencesTransaction(
                "job", "root", List.of("file1.parquet", "file2.parquet"),
                fileFactory.rootFile("file3.parquet", 100), updateTime);

        // When / Then
        whenSerDeThenMatchAndVerify(schema, transaction);
    }

    @Test
    void shouldSerDeSplitFileReferences() {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "p")
                .splitToNewChildren("L", "LL", "LR", "g")
                .splitToNewChildren("R", "RL", "RR", "u")
                .buildTree();
        Instant updateTime = Instant.parse("2023-03-26T10:05:01Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, updateTime);
        FileReferenceTransactionGeneric transaction = new SplitFileReferencesTransaction(List.of(
                splitFileToChildPartitions(
                        fileFactory.rootFile("file1.parquet", 100), "L", "R"),
                splitFileToChildPartitions(
                        fileFactory.partitionFile("L", "file2.parquet", 200), "LL", "LR")),
                updateTime);

        // When / Then
        whenSerDeThenMatchAndVerify(schema, transaction);
    }

    @Test
    void shouldSerDeSplitPartition() {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "p")
                .splitToNewChildren("L", "LL", "LR", "g")
                .buildTree();
        StateStoreTransaction transaction = new SplitPartitionTransaction(
                partitions.getPartition("L"),
                List.of(partitions.getPartition("LL"), partitions.getPartition("LR")));

        // When / Then
        whenSerDeThenMatchAndVerify(schema, transaction);
    }
}
