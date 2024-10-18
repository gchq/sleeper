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
package sleeper.ingest.impl.partitionfilewriter;

import sleeper.core.statestore.FileReference;

/**
 * A utility class providing static functions that are useful when wrtiting partition files.
 */
public class PartitionFileWriterUtils {
    /**
     * This class should not be instantiated.
     */
    private PartitionFileWriterUtils() {
    }

    /**
     * Create a reference to a new file to add to the state store. This should be passed to
     * {@link sleeper.core.statestore.StateStore.addFile}.
     *
     * @param  filename        the full path to the file, including file system
     * @param  partitionId     the ID of the partition the reference should be added to
     * @param  numberOfRecords the number of records in the file
     * @return                 the {@link FileReference} object
     */
    public static FileReference createFileReference(
            String filename, String partitionId, long numberOfRecords) {
        return FileReference.builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(numberOfRecords)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
    }
}
