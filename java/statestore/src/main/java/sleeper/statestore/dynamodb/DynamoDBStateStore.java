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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;

/**
 * An implementation of {@link StateStore} that uses DynamoDB to store the state.
 */
public class DynamoDBStateStore implements StateStore {

    public static final String FILE_NAME = DynamoDBFileInfoFormat.NAME;
    public static final String FILE_STATUS = DynamoDBFileInfoFormat.STATUS;
    public static final String FILE_PARTITION = DynamoDBFileInfoFormat.PARTITION;
    public static final String PARTITION_ID = DynamoDBPartitionFormat.ID;

    private final List<PrimitiveType> rowKeyTypes;
    private final DynamoDBFilesStore filesStore;
    private final DynamoDBPartitionsStore partitionsStore;

    public DynamoDBStateStore(TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        this(tableProperties.get(ACTIVE_FILEINFO_TABLENAME),
                tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME),
                tableProperties.get(PARTITION_TABLENAME),
                tableProperties.getSchema(),
                tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION),
                tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS),
                dynamoDB);
    }

    public DynamoDBStateStore(String activeFileInfoTablename,
                              String readyForGCFileInfoTablename,
                              String partitionTablename,
                              Schema schema,
                              int garbageCollectorDelayBeforeDeletionInSeconds,
                              boolean stronglyConsistentReads,
                              AmazonDynamoDB dynamoDB) {
        this.rowKeyTypes = schema.getRowKeyTypes();
        if (this.rowKeyTypes.isEmpty()) {
            throw new IllegalArgumentException("rowKeyTypes must not be empty");
        }
        this.filesStore = DynamoDBFilesStore.builder()
                .dynamoDB(dynamoDB).schema(schema)
                .activeTablename(activeFileInfoTablename).readyForGCTablename(readyForGCFileInfoTablename)
                .stronglyConsistentReads(stronglyConsistentReads)
                .garbageCollectorDelayBeforeDeletionInSeconds(garbageCollectorDelayBeforeDeletionInSeconds)
                .build();
        this.partitionsStore = DynamoDBPartitionsStore.builder()
                .dynamoDB(dynamoDB).schema(schema)
                .tableName(partitionTablename).stronglyConsistentReads(stronglyConsistentReads)
                .build();
    }

    @Override
    public List<PrimitiveType> getRowKeyTypes() {
        return Collections.unmodifiableList(rowKeyTypes);
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        filesStore.addFile(fileInfo);
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        filesStore.addFiles(fileInfos);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(
            List<FileInfo> filesToBeMarkedReadyForGC,
            FileInfo newActiveFile) throws StateStoreException {
        filesStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, newActiveFile);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
            List<FileInfo> filesToBeMarkedReadyForGC, FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        filesStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
    }

    /**
     * Atomically updates the job field of the given files to the given id, as long as
     * the compactionJob field is currently null.
     */
    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> files)
            throws StateStoreException {
        filesStore.atomicallyUpdateJobStatusOfFiles(jobId, files);
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException {
        filesStore.deleteReadyForGCFile(fileInfo);
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        return filesStore.getActiveFiles();
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() {
        return filesStore.getReadyForGCFiles();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        return filesStore.getActiveFilesWithNoJobId();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        return filesStore.getPartitionToActiveFilesMap();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(
            Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
        partitionsStore.atomicallyUpdatePartitionAndCreateNewOnes(splitPartition, newPartition1, newPartition2);
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitionsStore.getAllPartitions();
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitionsStore.getLeafPartitions();
    }

    @Override
    public void initialise() throws StateStoreException {
        partitionsStore.initialise();
    }

    @Override
    public void initialise(List<Partition> initialPartitions) throws StateStoreException {
        partitionsStore.initialise(initialPartitions);
    }

}
