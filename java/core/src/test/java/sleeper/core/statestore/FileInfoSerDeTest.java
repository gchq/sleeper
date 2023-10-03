/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.core.statestore;

import org.junit.jupiter.api.Test;

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class FileInfoSerDeTest {

    @Test
    public void shouldSerialiseAndDeserialiseCorrectlyForIntKey() throws IOException {
        // Given
        FileInfo fileInfo = FileInfo.builder()
                .filename("abc")
                .rowKeyTypes(new IntType())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("id")
                .jobId("JOB")
                .build();
        FileInfoSerDe fileInfoSerDe = new FileInfoSerDe();

        // When
        byte[] serialised = fileInfoSerDe.serialiseFileInfo(fileInfo);
        FileInfo deserialised = fileInfoSerDe.deserialiseFileInfo(serialised);

        // Then
        assertThat(deserialised).isEqualTo(fileInfo);
    }

    @Test
    public void shouldSerialiseAndDeserialiseCorrectlyForLongKey() throws IOException {
        // Given
        FileInfo fileInfo = FileInfo.builder()
                .filename("abc")
                .rowKeyTypes(new LongType())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("id")
                .jobId("JOB")
                .build();
        FileInfoSerDe fileInfoSerDe = new FileInfoSerDe();

        // When
        byte[] serialised = fileInfoSerDe.serialiseFileInfo(fileInfo);
        FileInfo deserialised = fileInfoSerDe.deserialiseFileInfo(serialised);

        // Then
        assertThat(deserialised).isEqualTo(fileInfo);
    }

    @Test
    public void shouldSerialiseAndDeserialiseCorrectlyForStringKey() throws IOException {
        // Given
        FileInfo fileInfo = FileInfo.builder()
                .filename("abc")
                .rowKeyTypes(new StringType())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("id")
                .jobId("JOB")
                .build();
        FileInfoSerDe fileInfoSerDe = new FileInfoSerDe();

        // When
        byte[] serialised = fileInfoSerDe.serialiseFileInfo(fileInfo);
        FileInfo deserialised = fileInfoSerDe.deserialiseFileInfo(serialised);

        // Then
        assertThat(deserialised).isEqualTo(fileInfo);
    }

    @Test
    public void shouldSerialiseAndDeserialiseCorrectlyForByteArrayKey() throws IOException {
        // Given
        FileInfo fileInfo = FileInfo.builder()
                .filename("abc")
                .rowKeyTypes(new ByteArrayType())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("id")
                .jobId("JOB")
                .build();
        FileInfoSerDe fileInfoSerDe = new FileInfoSerDe();

        // When
        byte[] serialised = fileInfoSerDe.serialiseFileInfo(fileInfo);
        FileInfo deserialised = fileInfoSerDe.deserialiseFileInfo(serialised);

        // Then
        assertThat(deserialised).isEqualTo(fileInfo);
    }

    @Test
    public void shouldSerialiseAndDeserialiseCorrectlyForByteArrayAndStringKey() throws IOException {
        // Given
        FileInfo fileInfo = FileInfo.builder()
                .filename("abc")
                .rowKeyTypes(new ByteArrayType(), new StringType())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("id")
                .jobId("JOB")
                .build();
        FileInfoSerDe fileInfoSerDe = new FileInfoSerDe();

        // When
        byte[] serialised = fileInfoSerDe.serialiseFileInfo(fileInfo);
        FileInfo deserialised = fileInfoSerDe.deserialiseFileInfo(serialised);

        // Then
        assertThat(deserialised).isEqualTo(fileInfo);
    }
}
