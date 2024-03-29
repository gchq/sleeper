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

public class SplitFileReference {

    private SplitFileReference() {
    }

    /**
     * Used to create a new reference to a file in one of the two child partitions that the original reference is
     * associated with. This will be paired with another call to this method for the other child partition, to split
     * the original file reference into two. The original reference should then be deleted.
     * <p>
     * This may be used to consider that file to be in the child partition without moving or copying the file. This is
     * useful when compacting parts of a file at a time down the partition tree to leaf partitions.
     * <p>
     * To split a file further down the tree this split must be repeated. This will compute an estimate of the number of
     * records in the file that are in this partition.
     *
     * @param  file             The file reference being split
     * @param  childPartitionId The ID of the child partition to create metadata for
     * @return                  The reference to the new copy
     */
    public static FileReference referenceForChildPartition(FileReference file, String childPartitionId) {
        return referenceForChildPartition(file, childPartitionId, file.getNumberOfRecords() / 2);
    }

    public static FileReference referenceForChildPartition(FileReference file, String childPartitionId, long numberOfRecords) {
        return FileReference.builder()
                .partitionId(childPartitionId)
                .filename(file.getFilename())
                .numberOfRecords(numberOfRecords)
                .countApproximate(true)
                .onlyContainsDataForThisPartition(false)
                .build();
    }
}
