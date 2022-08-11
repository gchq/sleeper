/*
 * Copyright 2022 Crown Copyright
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

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import sleeper.core.key.Key;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

public class FileInfoSerDeTest {

    @Test
    public void shouldSerialiseAndDeserialiseCorrectlyForIntKey() throws IOException {
        // Given
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFilename("abc");
        fileInfo.setRowKeyTypes(new IntType());
        fileInfo.setMinRowKey(Key.create(1));
        fileInfo.setMaxRowKey(Key.create(10));
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setNumberOfRecords(100L);
        fileInfo.setPartitionId("id");
        fileInfo.setJobId("JOB");
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
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFilename("abc");
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setNumberOfRecords(100L);
        fileInfo.setPartitionId("id");
        fileInfo.setJobId("JOB");
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
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFilename("abc");
        fileInfo.setRowKeyTypes(new StringType());
        fileInfo.setMinRowKey(Key.create("1"));
        fileInfo.setMaxRowKey(Key.create("10"));
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setNumberOfRecords(100L);
        fileInfo.setPartitionId("id");
        fileInfo.setJobId("JOB");
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
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFilename("abc");
        fileInfo.setRowKeyTypes(new ByteArrayType());
        fileInfo.setMinRowKey(Key.create(new byte[]{}));
        fileInfo.setMaxRowKey(Key.create(new byte[]{64, 64}));
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setNumberOfRecords(100L);
        fileInfo.setPartitionId("id");
        fileInfo.setJobId("JOB");
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
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFilename("abc");
        fileInfo.setRowKeyTypes(new ByteArrayType(), new StringType());
        fileInfo.setMinRowKey(Key.create(Arrays.asList(new byte[]{}, "A")));
        fileInfo.setMaxRowKey(Key.create(Arrays.asList(new byte[]{64, 64}, "Z")));
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setNumberOfRecords(100L);
        fileInfo.setPartitionId("id");
        fileInfo.setJobId("JOB");
        FileInfoSerDe fileInfoSerDe = new FileInfoSerDe();

        // When
        byte[] serialised = fileInfoSerDe.serialiseFileInfo(fileInfo);
        FileInfo deserialised = fileInfoSerDe.deserialiseFileInfo(serialised);

        // Then
        assertThat(deserialised).isEqualTo(fileInfo);
    }
}
