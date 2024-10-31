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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.LoggedDuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithReferences;

public class StateStoreFilesArrowFormatTest {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreFilesArrowFormatTest.class);

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
        AllReferencesToAFile file = AllReferencesToAFile.fileWithOneReference(reference, updateTime);
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
        AllReferencesToAFile file = AllReferencesToAFile.fileWithOneReference(reference, updateTime);
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

    @Test
    void shouldWriteManyFiles() throws Exception {
        // Given
        Instant startTime = Instant.now();
        Instant updateTime = Instant.parse("2024-05-28T13:25:01.123Z");
        List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(
                IntStream.rangeClosed(1, 1_000)
                        .mapToObj(partitionNumber -> partitionNumber)
                        .flatMap(partitionNumber -> IntStream.rangeClosed(1, 100)
                                .mapToObj(fileNumber -> FileReference.builder()
                                        .filename("file-" + fileNumber + ".parquet")
                                        .partitionId("partition-" + partitionNumber)
                                        .numberOfRecords((long) fileNumber)
                                        .countApproximate(false)
                                        .onlyContainsDataForThisPartition(true)
                                        .build())))
                .map(file -> file.withCreatedUpdateTime(updateTime))
                .collect(toUnmodifiableList());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(100_000_000);

        // When
        Instant generatedTime = Instant.now();
        write(files, bytes);

        // Then
        Instant writtenTime = Instant.now();
        List<AllReferencesToAFile> found = read(bytes);
        Instant readTime = Instant.now();
        assertThat(found).isEqualTo(files);
        Instant endTime = Instant.now();
        LOGGER.info("Started at {}", startTime);
        LOGGER.info("Generated at {}, took {}", generatedTime, LoggedDuration.withFullOutput(startTime, generatedTime));
        LOGGER.info("Wrote {} bytes at {}, took {}", bytes.size(), writtenTime, LoggedDuration.withFullOutput(generatedTime, writtenTime));
        LOGGER.info("Read at {}, took {}", readTime, LoggedDuration.withFullOutput(writtenTime, readTime));
        LOGGER.info("Asserted at {}, took {}", endTime, LoggedDuration.withFullOutput(readTime, endTime));
    }

    private void write(List<AllReferencesToAFile> files, ByteArrayOutputStream stream) throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            LOGGER.info("Opened allocator");
            StateStoreFilesArrowFormat.write(files, allocator, Channels.newChannel(stream));
        }
    }

    private List<AllReferencesToAFile> read(ByteArrayOutputStream stream) throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            return StateStoreFilesArrowFormat.read(allocator,
                    Channels.newChannel(new ByteArrayInputStream(stream.toByteArray())));
        }
    }
}
