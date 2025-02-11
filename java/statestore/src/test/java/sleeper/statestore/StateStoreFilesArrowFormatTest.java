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
package sleeper.statestore;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.statestore.StateStoreFilesArrowFormat.ReadResult;
import sleeper.statestore.StateStoreFilesArrowFormat.WriteResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithOneReference;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithReferences;

public class StateStoreFilesArrowFormatTest {

    private final BufferAllocator allocator = new RootAllocator();

    @Test
    void shouldWriteOneFileWithNoReferences() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-05-28T13:25:01.123Z");
        AllReferencesToAFile file = fileWithNoReferences("test.parquet", updateTime);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(List.of(file), bytes);

        // Then
        assertThat(read(bytes)).containsExactly(file);
    }

    @Test
    void shouldWriteTwoFilesWithNoReferences() throws Exception {
        // Given
        AllReferencesToAFile file1 = fileWithNoReferences(
                "file1.parquet", Instant.parse("2024-05-28T14:57:01.123Z"));
        AllReferencesToAFile file2 = fileWithNoReferences(
                "file2.parquet", Instant.parse("2024-05-28T14:58:01.123Z"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(List.of(file1, file2), bytes);

        // Then
        assertThat(read(bytes)).containsExactly(file1, file2);
    }

    @Test
    void shouldWriteOneFileWithOneReference() throws Exception {
        // Given
        FileReference reference = wholeFileReferenceBuilder()
                .filename("test.parquet")
                .partitionId("root")
                .numberOfRecords(123L)
                .jobId("test-job")
                .lastStateStoreUpdateTime(Instant.parse("2024-05-28T13:25:02.123Z"))
                .build();
        Instant updateTime = Instant.parse("2024-05-28T13:25:01.123Z");
        AllReferencesToAFile file = fileWithOneReference(reference, updateTime);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(List.of(file), bytes);

        // Then
        assertThat(read(bytes)).containsExactly(file);
    }

    @Test
    void shouldWriteFileReferenceWithNoJob() throws Exception {
        // Given
        FileReference reference = wholeFileReferenceBuilder()
                .filename("test.parquet")
                .partitionId("root")
                .numberOfRecords(123L)
                .build();
        Instant updateTime = Instant.parse("2024-05-28T13:25:01.123Z");
        AllReferencesToAFile file = fileWithOneReference(reference, updateTime);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(List.of(file), bytes);

        // Then
        assertThat(read(bytes)).containsExactly(file);
    }

    @Test
    void shouldWriteOneFileWithTwoReferences() throws Exception {
        // Given
        FileReference reference1 = partitialFileReferenceBuilder()
                .filename("file.parquet")
                .partitionId("A")
                .numberOfRecords(123L)
                .jobId("test-job-1")
                .build();
        FileReference reference2 = partitialFileReferenceBuilder()
                .filename("file.parquet")
                .partitionId("B")
                .numberOfRecords(456L)
                .jobId("test-job-2")
                .build();
        AllReferencesToAFile file = fileWithReferences(reference1, reference2)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:01.123Z"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(List.of(file), bytes);

        // Then
        assertThat(read(bytes)).containsExactly(file);
    }

    @Test
    void shouldWriteNoFiles() throws Exception {
        // Given
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(List.of(), bytes);

        // Then
        assertThat(read(bytes)).isEmpty();
    }

    @Test
    void shouldWriteMoreFilesThanBatchSize() throws Exception {
        // Given
        FileReference reference1 = wholeFileReferenceBuilder()
                .filename("file1.parquet")
                .partitionId("A")
                .numberOfRecords(123L)
                .jobId("test-job-1")
                .build();
        FileReference reference2 = wholeFileReferenceBuilder()
                .filename("file2.parquet")
                .partitionId("B")
                .numberOfRecords(456L)
                .jobId("test-job-2")
                .build();
        FileReference reference3 = wholeFileReferenceBuilder()
                .filename("file3.parquet")
                .partitionId("C")
                .numberOfRecords(789L)
                .build();
        AllReferencesToAFile file1 = fileWithReferences(reference1)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:01.123Z"));
        AllReferencesToAFile file2 = fileWithReferences(reference2)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:02.123Z"));
        AllReferencesToAFile file3 = fileWithReferences(reference3)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:03.123Z"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, List.of(file1, file2, file3), bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(streamFiles(readResult)).containsExactly(file1, file2, file3);
        assertThat(writeResult.numBatches()).isEqualTo(2).isEqualTo(readResult.numBatches());
        assertThat(writeResult.numReferences()).isEqualTo(3).isEqualTo(readResult.numReferences());
    }

    @Test
    void shouldMoveToNextBatchWhenOneFileFillsBatch() throws Exception {
        // Given
        FileReference reference1 = partitialFileReferenceBuilder()
                .filename("file1.parquet")
                .partitionId("A")
                .numberOfRecords(123L)
                .build();
        FileReference reference2 = partitialFileReferenceBuilder()
                .filename("file1.parquet")
                .partitionId("B")
                .numberOfRecords(456L)
                .build();
        FileReference reference3 = wholeFileReferenceBuilder()
                .filename("file2.parquet")
                .partitionId("C")
                .numberOfRecords(789L)
                .build();
        AllReferencesToAFile file1 = fileWithReferences(reference1, reference2)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:01.123Z"));
        AllReferencesToAFile file2 = fileWithReferences(reference3)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:02.123Z"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, List.of(file1, file2), bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(streamFiles(readResult)).containsExactly(file1, file2);
        assertThat(writeResult.numBatches()).isEqualTo(2).isEqualTo(readResult.numBatches());
        assertThat(writeResult.numReferences()).isEqualTo(3).isEqualTo(readResult.numReferences());
    }

    @Test
    void shouldFillNextBatchAfterOneFileOverflowsBatch() throws Exception {
        // Given
        FileReference file1Ref1 = partitialFileReferenceBuilder()
                .filename("file1.parquet")
                .partitionId("A")
                .numberOfRecords(123L)
                .build();
        FileReference file1Ref2 = partitialFileReferenceBuilder()
                .filename("file1.parquet")
                .partitionId("B")
                .numberOfRecords(456L)
                .build();
        FileReference file1Ref3 = partitialFileReferenceBuilder()
                .filename("file1.parquet")
                .partitionId("C")
                .numberOfRecords(789L)
                .build();
        FileReference file2Ref = wholeFileReferenceBuilder()
                .filename("file2.parquet")
                .partitionId("D")
                .numberOfRecords(123L)
                .build();
        FileReference file3Ref = wholeFileReferenceBuilder()
                .filename("file3.parquet")
                .partitionId("E")
                .numberOfRecords(456L)
                .build();
        AllReferencesToAFile file1 = fileWithReferences(file1Ref1, file1Ref2, file1Ref3)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:01.123Z"));
        AllReferencesToAFile file2 = fileWithReferences(file2Ref)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:02.123Z"));
        AllReferencesToAFile file3 = fileWithReferences(file3Ref)
                .withCreatedUpdateTime(Instant.parse("2024-05-28T13:25:03.123Z"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, List.of(file1, file2, file3), bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(streamFiles(readResult)).containsExactly(file1, file2, file3);
        assertThat(writeResult.numBatches()).isEqualTo(2).isEqualTo(readResult.numBatches());
        assertThat(writeResult.numReferences()).isEqualTo(5).isEqualTo(readResult.numReferences());
    }

    private WriteResult write(List<AllReferencesToAFile> files, ByteArrayOutputStream stream) throws Exception {
        return writeWithMaxElementsInBatch(10, files, stream);
    }

    private WriteResult writeWithMaxElementsInBatch(int maxElementsInBatch, List<AllReferencesToAFile> files, ByteArrayOutputStream stream) throws Exception {
        StateStoreFiles state = new StateStoreFiles();
        files.forEach(file -> state.add(StateStoreFile.from(file)));
        return StateStoreFilesArrowFormat.write(state, allocator, Channels.newChannel(stream), maxElementsInBatch);
    }

    private Stream<AllReferencesToAFile> read(ByteArrayOutputStream stream) throws Exception {
        return streamFiles(readResult(stream));
    }

    private ReadResult readResult(ByteArrayOutputStream stream) throws Exception {
        return StateStoreFilesArrowFormat.read(allocator,
                Channels.newChannel(new ByteArrayInputStream(stream.toByteArray())));
    }

    private Stream<AllReferencesToAFile> streamFiles(ReadResult result) {
        return result.files().referencedAndUnreferenced().stream().map(StateStoreFile::toModel);
    }

    private FileReference.Builder partitialFileReferenceBuilder() {
        return FileReference.builder()
                .countApproximate(true)
                .onlyContainsDataForThisPartition(false);
    }

    private FileReference.Builder wholeFileReferenceBuilder() {
        return FileReference.builder()
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true);
    }
}
