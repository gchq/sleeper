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
package sleeper.core.statestore;

import java.time.Duration;
import java.time.Instant;

public class FileReferenceTestData {
    private FileReferenceTestData() {
    }

    public static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2023-10-04T14:08:00Z");
    public static final Instant AFTER_DEFAULT_UPDATE_TIME = DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(1));
    public static final long DEFAULT_NUMBER_OF_RECORDS = 100L;

    public static FileReference defaultFileOnRootPartition(String filename) {
        return defaultFileOnRootPartitionWithRecords(filename, DEFAULT_NUMBER_OF_RECORDS);
    }

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

    public static FileReference splitFile(FileReference parentFile, String childPartitionId) {
        return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    public static FileReference withLastUpdate(Instant updateTime, FileReference file) {
        return file.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }

    public static FileReference withJobId(String jobId, FileReference file) {
        return file.toBuilder().jobId(jobId).build();
    }
}
