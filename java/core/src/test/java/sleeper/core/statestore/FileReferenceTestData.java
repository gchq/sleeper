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
package sleeper.core.statestore;

import java.time.Duration;
import java.time.Instant;

/**
 * Test helpers for setting up test file references.
 */
public class FileReferenceTestData {
    private FileReferenceTestData() {
    }

    public static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2023-10-04T14:08:00Z");
    public static final Instant AFTER_DEFAULT_UPDATE_TIME = DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(1));
    public static final long DEFAULT_NUMBER_OF_RECORDS = 100L;

    /**
     * Sets up a file reference on the root partition. Only use this when most fields won't matter, including the
     * partition ID, the state store update time. For most use cases {@link FileReferenceFactory} is preferable.
     *
     * @param  filename the filename
     * @return          the file reference
     */
    public static FileReference defaultFileOnRootPartition(String filename) {
        return defaultFileOnRootPartitionWithRecords(filename, DEFAULT_NUMBER_OF_RECORDS);
    }

    /**
     * Sets up a file reference on the root partition. Only use this when most fields won't matter, including the
     * partition ID, the state store update time. For most use cases {@link FileReferenceFactory} is preferable.
     *
     * @param  filename the filename
     * @param  records  the number of records in the file
     * @return          the file reference
     */
    public static FileReference defaultFileOnRootPartitionWithRecords(String filename, long records) {
        return FileReference.builder()
                .filename(filename)
                .partitionId("root")
                .numberOfRecords(records)
                .lastStateStoreUpdateTime(Instant.parse("2022-12-08T11:03:00.001Z"))
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
    }

    /**
     * Creates a new reference to a file on a child partition. Used when splitting a parent file reference into exactly
     * two. This must split to a direct child partition of the one the file is originally referenced on.
     *
     * @param  parentFile       the parent file being split
     * @param  childPartitionId the ID of the partition to reference the file on
     * @return                  the file reference
     */
    public static FileReference splitFile(FileReference parentFile, String childPartitionId) {
        return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    /**
     * Creates a copy of a file reference with the specified last update time.
     *
     * @param  updateTime the update time
     * @param  file       the file reference
     * @return            the copy
     */
    public static FileReference withLastUpdate(Instant updateTime, FileReference file) {
        return file.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }

    /**
     * Creates a copy of a file reference assigned to the specified job.
     *
     * @param  jobId the ID of the job
     * @param  file  the file reference
     * @return       the copy
     */
    public static FileReference withJobId(String jobId, FileReference file) {
        return file.toBuilder().jobId(jobId).build();
    }
}
