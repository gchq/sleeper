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
import sleeper.core.statestore.transactionlog.StateStoreFile;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

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
        FileReference reference = FileReference.builder()
                .filename("test.parquet")
                .partitionId("root")
                .numberOfRecords(123L)
                .jobId("test-job")
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
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
        FileReference reference = FileReference.builder()
                .filename("test.parquet")
                .partitionId("root")
                .numberOfRecords(123L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
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
        FileReference reference1 = FileReference.builder()
                .filename("file.parquet")
                .partitionId("A")
                .numberOfRecords(123L)
                .jobId("test-job-1")
                .countApproximate(true)
                .onlyContainsDataForThisPartition(false)
                .build();
        FileReference reference2 = FileReference.builder()
                .filename("file.parquet")
                .partitionId("B")
                .numberOfRecords(456L)
                .jobId("test-job-2")
                .countApproximate(true)
                .onlyContainsDataForThisPartition(false)
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

    private void write(List<AllReferencesToAFile> files, ByteArrayOutputStream stream) throws Exception {
        StateStoreFiles state = new StateStoreFiles();
        files.forEach(file -> state.add(StateStoreFile.from(file)));
        StateStoreFilesArrowFormat.write(state, allocator, Channels.newChannel(stream));
    }

    private Stream<AllReferencesToAFile> read(ByteArrayOutputStream stream) throws Exception {
        return StateStoreFilesArrowFormat.read(allocator,
                Channels.newChannel(new ByteArrayInputStream(stream.toByteArray())))
                .referencedAndUnreferenced().stream().map(StateStoreFile::toModel);
    }
}
