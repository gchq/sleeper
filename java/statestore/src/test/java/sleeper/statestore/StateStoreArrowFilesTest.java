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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreArrowFilesTest {

    @Test
    @Disabled("TODO")
    void shouldWriteOneFileInArrowFormat() {
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

    private void write(List<AllReferencesToAFile> files, ByteArrayOutputStream stream) {
        StateStoreArrowFiles.write(files, Channels.newChannel(stream));
    }

    private List<AllReferencesToAFile> read(ByteArrayOutputStream stream) {
        return StateStoreArrowFiles.read(Channels.newChannel(
                new ByteArrayInputStream(stream.toByteArray())))
                .collect(toUnmodifiableList());
    }

}
