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
package sleeper.core.statestore.transactionlog;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Holds the state of references to a file, for a state store backed by a transaction log. This object is mutable, is
 * cached in memory in the state store, and is updated by applying each transaction in the log in sequence. This is not
 * thread safe.
 * <p>
 * The methods to update this object should only ever be called by the transactions.
 */
public class StateStoreFile {

    private final String filename;
    private Instant lastStateStoreUpdateTime;
    private final Map<String, FileReference> referenceByPartitionId = new TreeMap<>();

    private StateStoreFile(String filename, Instant lastStateStoreUpdateTime, Collection<FileReference> references) {
        this.filename = filename;
        this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
        references.forEach(reference -> referenceByPartitionId.put(reference.getPartitionId(), reference));
    }

    /**
     * Creates an instance of this class from an immutable model.
     *
     * @param  model the model of the file
     * @return       the instance
     */
    public static StateStoreFile from(AllReferencesToAFile model) {
        return new StateStoreFile(model.getFilename(), model.getLastStateStoreUpdateTime(), model.getReferences());
    }

    /**
     * Converts this object to the immutable model.
     *
     * @return an instance of the immutable model
     */
    public AllReferencesToAFile toModel() {
        return AllReferencesToAFile.builder()
                .filename(filename)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime)
                .references(referenceByPartitionId.values())
                .build();
    }

    /**
     * Removes the reference on one partition, and replaces it with new references. This is used to move the file down
     * the tree of partitions. The new references should cover all the records that were previously covered by the
     * reference that's being removed.
     *
     * @param partitionId   the ID of the partition to remove the file from
     * @param newReferences the references to add
     * @param updateTime    the update time that this occurs
     */
    public void splitReferenceFromPartition(
            String partitionId, Collection<FileReference> newReferences, Instant updateTime) {
        referenceByPartitionId.remove(partitionId);
        newReferences.forEach(reference -> referenceByPartitionId.put(reference.getPartitionId(),
                reference.toBuilder().lastStateStoreUpdateTime(updateTime).build()));
        lastStateStoreUpdateTime = updateTime;
    }

    /**
     * Removes the reference to this file on a partition. This is used when adding the output of a compaction in a new
     * file that contains all the records for a certain partition. This means that the input files for the compaction
     * must no longer be referenced in that partition.
     *
     * @param partitionId the ID of the partition to remove the file from
     * @param updateTime  the update time that this occurs (should be set by the state store implementation)
     */
    public void removeReferenceForPartition(String partitionId, Instant updateTime) {
        referenceByPartitionId.remove(partitionId);
        lastStateStoreUpdateTime = updateTime;
    }

    /**
     * Assigns the reference to this file on one partition to a job. This is used when assigning a compaction job to its
     * input files. Note that parts of a file are assigned to jobs independently. Each partition that a file is in
     * covers different records. Each reference on each partition will be assigned to and processed by a different job.
     *
     * @param jobId       the ID of the job to assign the file reference to
     * @param partitionId the ID of the partition whose reference should be assigned to the job
     * @param updateTime  the update time that this occurs (should be set by the state store implementation)
     */
    public void setJobIdForPartition(String jobId, String partitionId, Instant updateTime) {
        referenceByPartitionId.put(partitionId,
                referenceByPartitionId.get(partitionId).toBuilder()
                        .jobId(jobId).lastStateStoreUpdateTime(updateTime).build());
        lastStateStoreUpdateTime = updateTime;
    }

}
