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
package sleeper.statestore.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreArrowFileStore;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.statestore.s3.S3StateStore.CURRENT_PARTITIONS_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStoreDataFile.conditionCheckFor;

/**
 * A Sleeper table partition store where the state is held in S3, and revisions of the state are indexed in DynamoDB.
 */
class S3PartitionStore implements PartitionStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3PartitionStore.class);

    private final S3RevisionIdStore s3RevisionIdStore;
    private final Configuration conf;
    private final Schema tableSchema;
    private final String stateStorePath;
    private final S3StateStoreDataFile<Map<String, Partition>> s3StateStoreFile;
    private final StateStoreArrowFileStore dataStore;

    private S3PartitionStore(Builder builder) {
        conf = Objects.requireNonNull(builder.conf, "hadoopConfiguration must not be null");
        tableSchema = Objects.requireNonNull(builder.tableSchema, "tableSchema must not be null");
        stateStorePath = Objects.requireNonNull(builder.stateStorePath, "stateStorePath must not be null");
        s3RevisionIdStore = Objects.requireNonNull(builder.s3RevisionIdStore, "s3RevisionIdStore must not be null");
        s3StateStoreFile = S3StateStoreDataFile.builder()
                .revisionStore(s3RevisionIdStore)
                .description("partitions")
                .revisionIdKey(CURRENT_PARTITIONS_REVISION_ID_KEY)
                .buildPathFromRevisionId(this::getPartitionsPath)
                .loadAndWriteData(this::readPartitionsMap, this::writePartitionsMap)
                .hadoopConf(conf)
                .build();
        dataStore = new StateStoreArrowFileStore(conf);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
        s3StateStoreFile.updateWithAttempts(5,
                partitionIdToPartition -> {
                    partitionIdToPartition.put(splitPartition.getId(), splitPartition);
                    partitionIdToPartition.put(newPartition1.getId(), newPartition1);
                    partitionIdToPartition.put(newPartition2.getId(), newPartition2);
                    return partitionIdToPartition;
                },
                conditionCheckFor(partitionIdToPartition -> validateSplitPartitionRequest(
                        partitionIdToPartition, splitPartition, newPartition1, newPartition2)));
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        S3RevisionId revisionId = s3RevisionIdStore.getCurrentPartitionsRevisionId();
        if (null == revisionId) {
            return Collections.emptyList();
        }
        String path = getPartitionsPath(revisionId);
        return readPartitions(path);
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        // TODO Optimise by passing the leaf predicate down
        return getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
    }

    private static String validateSplitPartitionRequest(Map<String, Partition> partitionIdToPartition,
            Partition splitPartition,
            Partition newPartition1,
            Partition newPartition2) {
        // Validate that splitPartition is there and is a leaf partition
        if (!partitionIdToPartition.containsKey(splitPartition.getId())) {
            return "splitPartition should be present";
        }
        if (!partitionIdToPartition.get(splitPartition.getId()).isLeafPartition()) {
            return "splitPartition should be a leaf partition";
        }

        // Validate that newPartition1 and newPartition2 are not already there
        if (partitionIdToPartition.containsKey(newPartition1.getId()) || partitionIdToPartition.containsKey(newPartition2.getId())) {
            return "newPartition1 and newPartition2 should not be present";
        }

        // Validate that the children of splitPartition are newPartition1 and newPartition2
        Set<String> splitPartitionChildrenIds = new HashSet<>(splitPartition.getChildPartitionIds());
        Set<String> newIds = new HashSet<>();
        newIds.add(newPartition1.getId());
        newIds.add(newPartition2.getId());
        if (!splitPartitionChildrenIds.equals(newIds)) {
            return "Children of splitPartition do not equal newPartition1 and new Partition2";
        }

        // Validate that the parent of newPartition1 and newPartition2 are correct
        if (!newPartition1.getParentPartitionId().equals(splitPartition.getId())) {
            return "Parent of newPartition1 does not equal splitPartition";
        }
        if (!newPartition2.getParentPartitionId().equals(splitPartition.getId())) {
            return "Parent of newPartition2 does not equal splitPartition";
        }

        // Validate that newPartition1 and newPartition2 are leaf partitions
        if (!newPartition1.isLeafPartition() || !newPartition2.isLeafPartition()) {
            return "newPartition1 and newPartition2 should be leaf partitions";
        }
        return "";
    }

    @Override
    public void clearPartitionData() throws StateStoreException {
        try {
            Path path = new Path(stateStorePath + "/partitions");
            path.getFileSystem(conf).delete(path, true);
            s3RevisionIdStore.deletePartitionsRevision();
        } catch (IOException | RuntimeException e) {
            throw new StateStoreException("Failed deleting partitions file", e);
        }
    }

    private String getPartitionsPath(S3RevisionId revisionId) {
        return stateStorePath + "/partitions/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-partitions.arrow";
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(tableSchema, Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        if (null == partitions || partitions.isEmpty()) {
            throw new StateStoreException("At least one partition must be provided");
        }
        setPartitions(partitions);
    }

    private void setPartitions(List<Partition> partitions) throws StateStoreException {
        // Write partitions to file
        S3RevisionId revisionId = S3RevisionId.firstRevision(UUID.randomUUID().toString());
        String path = getPartitionsPath(revisionId);
        LOGGER.debug("Writing initial partition information (revisionId = {}, path = {})", revisionId, path);
        writePartitions(partitions, path);

        // Update Dynamo
        s3RevisionIdStore.saveFirstPartitionRevision(revisionId);
    }

    private Map<String, Partition> readPartitionsMap(String path) throws StateStoreException {
        return getMapFromPartitionIdToPartition(readPartitions(path));
    }

    private void writePartitionsMap(Map<String, Partition> partitionsById, String path) throws StateStoreException {
        writePartitions(partitionsById.values(), path);
    }

    private Map<String, Partition> getMapFromPartitionIdToPartition(List<Partition> partitions) throws StateStoreException {
        Map<String, Partition> partitionIdToPartition = new HashMap<>();
        for (Partition partition : partitions) {
            if (partitionIdToPartition.containsKey(partition.getId())) {
                throw new StateStoreException("Error: found two partitions with the same id ("
                        + partition + "," + partitionIdToPartition.get(partition.getId()) + ")");
            }
            partitionIdToPartition.put(partition.getId(), partition);
        }
        return partitionIdToPartition;
    }

    private void writePartitions(Collection<Partition> partitions, String path) throws StateStoreException {
        try {
            dataStore.savePartitions(path, partitions, tableSchema);
        } catch (IOException e) {
            throw new StateStoreException("Failed to save partitions", e);
        }
    }

    private List<Partition> readPartitions(String path) throws StateStoreException {
        try {
            return dataStore.loadPartitions(path, tableSchema);
        } catch (IOException e) {
            throw new StateStoreException("Failed to load partitions", e);
        }
    }

    /**
     * Builder to create a partition store backed by S3.
     */
    static final class Builder {
        private Configuration conf;
        private Schema tableSchema;
        private String stateStorePath;
        private S3RevisionIdStore s3RevisionIdStore;

        private Builder() {
        }

        Builder conf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        Builder tableSchema(Schema tableSchema) {
            this.tableSchema = tableSchema;
            return this;
        }

        Builder stateStorePath(String stateStorePath) {
            this.stateStorePath = stateStorePath;
            return this;
        }

        Builder s3RevisionIdStore(S3RevisionIdStore s3RevisionIdStore) {
            this.s3RevisionIdStore = s3RevisionIdStore;
            return this;
        }

        S3PartitionStore build() {
            return new S3PartitionStore(this);
        }
    }
}
