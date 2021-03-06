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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FileInfoTest {

    @Test
    public void testSettersAndGetters() {
        // Given
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("0");
        fileInfo.setFilename("abc");
        fileInfo.setJobId("Job1");
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When / Then
        assertEquals(FileInfo.FileStatus.ACTIVE, fileInfo.getFileStatus());
        assertEquals("0", fileInfo.getPartitionId());
        assertEquals("abc", fileInfo.getFilename());
        assertEquals("Job1", fileInfo.getJobId());
        assertEquals(1_000_000L, fileInfo.getLastStateStoreUpdateTime().longValue());
    }

    @Test
    public void testEqualsAndHashCode() {
        // Given
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("0");
        fileInfo1.setFilename("abc");
        fileInfo1.setJobId("Job1");
        fileInfo1.setLastStateStoreUpdateTime(1_000_000L);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("0");
        fileInfo2.setFilename("abc");
        fileInfo2.setJobId("Job1");
        fileInfo2.setLastStateStoreUpdateTime(1_000_000L);
        FileInfo fileInfo3 = new FileInfo();
        fileInfo3.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo3.setPartitionId("0");
        fileInfo3.setFilename("abc");
        fileInfo3.setJobId("Job3");
        fileInfo3.setLastStateStoreUpdateTime(2_000_000L);

        // When / Then
        assertEquals(fileInfo1, fileInfo2);
        assertEquals(fileInfo1.hashCode(), fileInfo2.hashCode());
        assertNotEquals(fileInfo1, fileInfo3);
        assertNotEquals(fileInfo1.hashCode(), fileInfo3.hashCode());
    }
}
