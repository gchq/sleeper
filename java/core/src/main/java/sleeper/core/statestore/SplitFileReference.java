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

/**
 * Splits file references to create new references to the file that replace the original. Multiple method calls will
 * made for an original reference to produce the new references.
 * <p>
 * This may be used to consider a file to be in a child partition without moving or copying the file. This is
 * useful when compacting parts of a file at a time down the partition tree to leaf partitions. To split a file further
 * down the tree this split must be repeated.
 */
public class SplitFileReference {

    private SplitFileReference() {
    }

    /**
     * Creates a new reference to a file in one of the two child partitions that the original reference is associated
     * with. This will be paired with another call to this method for the other child partition, to split the original
     * file reference into two. The original reference should then be deleted.
     * <p>
     * This will estimate the number of records in the child partition by assuming an even split between two child
     * partitions.
     *
     * @param  file             the file reference being split
     * @param  childPartitionId the ID of the child partition to create metadata for
     * @return                  the reference to the new copy
     */
    public static FileReference referenceForChildPartition(FileReference file, String childPartitionId) {
        return referenceForChildPartition(file, childPartitionId, file.getNumberOfRecords() / 2);
    }

    /**
     * Creates a new reference to a file in one of the two child partitions that the original reference is associated
     * with. This will be paired with another call to this method for the other child partition, to split the original
     * file reference into two. The original reference should then be deleted.
     * <p>
     * This should be used when we have estimated the number of records in the child partiton.
     *
     * @param  file             the file reference being split
     * @param  childPartitionId the ID of the child partition to create metadata for
     * @param  numberOfRecords  the estimate of the number of records in the child partition
     * @return                  the reference to the new copy
     */
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
