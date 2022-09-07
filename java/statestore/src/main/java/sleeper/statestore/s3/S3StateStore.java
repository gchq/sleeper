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
package sleeper.statestore.s3;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.REVISION_TABLENAME;

/**
 * An implementation of {@link StateStore} that stores the information in Parquet files in S3. A DynamoDB table is
 * used as a lightweight consistency layer. The table stores a revision id for the current version of the files
 * information. This Dynamo value is conditionally updated when the state store is updated. If this conditional update
 * fails then the update is retried.
 */
public class S3StateStore implements StateStore {
    public static final String REVISION_ID_KEY = "REVISION_ID_KEY";
    public static final String CURRENT_PARTITIONS_REVISION_ID_KEY = "CURRENT_PARTITIONS_REVISION_ID_KEY";
    public static final String CURRENT_FILES_REVISION_ID_KEY = "CURRENT_FILES_REVISION_ID_KEY";
    public static final String CURRENT_REVISION = "CURRENT_REVISION";
    public static final String CURRENT_UUID = "CURRENT_UUID";

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StateStore.class);

    private final S3FileInfoStore fileInfoStore;
    private final S3PartitionStore partitionStore;
    private final Schema tableSchema;

    public S3StateStore(InstanceProperties instanceProperties,
                        TableProperties tableProperties,
                        AmazonDynamoDB dynamoDB,
                        Configuration conf) {
        this(instanceProperties.get(FILE_SYSTEM),
                instanceProperties.getInt(MAXIMUM_CONNECTIONS_TO_S3),
                tableProperties.get(DATA_BUCKET),
                tableProperties.get(REVISION_TABLENAME),
                tableProperties.getSchema(),
                tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION),
                dynamoDB,
                conf);
    }

    public S3StateStore(String fs,
                        int maxConnectionsToS3,
                        String s3Bucket,
                        String dynamoRevisionIdTable,
                        Schema tableSchema,
                        int garbageCollectorDelayBeforeDeletionInSeconds,
                        AmazonDynamoDB dynamoDB,
                        Configuration conf) {
        this.fileInfoStore = S3FileInfoStore.builder()
                .fs(fs)
                .s3Bucket(s3Bucket)
                .dynamoRevisionIdTable(dynamoRevisionIdTable)
                .rowKeyTypes(tableSchema.getRowKeyTypes())
                .garbageCollectorDelayBeforeDeletionInSeconds(garbageCollectorDelayBeforeDeletionInSeconds)
                .dynamoDB(dynamoDB)
                .conf(conf)
                .build();
        this.partitionStore = S3PartitionStore.builder()
                .fs(fs)
                .s3Bucket(s3Bucket)
                .dynamoRevisionIdTable(dynamoRevisionIdTable)
                .tableSchema(tableSchema)
                .dynamoDB(dynamoDB)
                .conf(conf)
                .build();
        this.tableSchema = tableSchema;
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        fileInfoStore.addFiles(Collections.singletonList(fileInfo));
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        fileInfoStore.addFiles(fileInfos);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo newActiveFile)
            throws StateStoreException {
        fileInfoStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, newActiveFile);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC,
                                                                         FileInfo leftFileInfo,
                                                                         FileInfo rightFileInfo) throws StateStoreException {
        fileInfoStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        fileInfoStore.atomicallyUpdateJobStatusOfFiles(jobId, fileInfos);
    }

    @Override
    public void deleteReadyForGCFile(FileInfo readyForGCFileInfo) throws StateStoreException {
        fileInfoStore.deleteReadyForGCFile(readyForGCFileInfo);
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        return fileInfoStore.getActiveFiles();
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() throws StateStoreException {
        return fileInfoStore.getReadyForGCFiles();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        return fileInfoStore.getActiveFilesWithNoJobId();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        return fileInfoStore.getPartitionToActiveFilesMap();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition,
                                                          Partition newPartition1,
                                                          Partition newPartition2) throws StateStoreException {
        partitionStore.atomicallyUpdatePartitionAndCreateNewOnes(splitPartition, newPartition1, newPartition2);
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitionStore.getAllPartitions();
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitionStore.getLeafPartitions();
    }

    @Override
    public void initialise() throws StateStoreException {
        partitionStore.initialise(new PartitionsFromSplitPoints(tableSchema, Collections.emptyList()).construct());
        fileInfoStore.initialise();
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        partitionStore.initialise(partitions);
        fileInfoStore.initialise();
    }

    public void setInitialFileInfos() throws StateStoreException {
        fileInfoStore.initialise();
    }

    protected static String getZeroPaddedLong(long number) {
        StringBuilder versionString = new StringBuilder("" + number);
        while (versionString.length() < 12) {
            versionString.insert(0, "0");
        }
        return versionString.toString();
    }
}
