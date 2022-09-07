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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.key.KeySerDe;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.RegionSerDe;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private final List<PrimitiveType> rowKeyTypes;
    private final int garbageCollectorDelayBeforeDeletionInSeconds;
    private final KeySerDe keySerDe;
    private final String fs;
    private final int maxConnectionsToS3;
    private final String s3Bucket;
    private final AmazonDynamoDB dynamoDB;
    private final String dynamoRevisionIdTable;
    private final Schema tableSchema;
    private final Schema fileSchema;
    private final Schema partitionSchema;
    private final Configuration conf;
    private final RegionSerDe regionSerDe;

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
        this.fs = Objects.requireNonNull(fs, "fs must not be null");
        this.maxConnectionsToS3 = maxConnectionsToS3;
        this.s3Bucket = Objects.requireNonNull(s3Bucket, "s3Bucket must not be null");
        this.dynamoRevisionIdTable = Objects.requireNonNull(dynamoRevisionIdTable, "dynamoRevisionIdTable must not be null");
        this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema must not be null");
        this.rowKeyTypes = this.tableSchema.getRowKeyTypes();
        this.garbageCollectorDelayBeforeDeletionInSeconds = garbageCollectorDelayBeforeDeletionInSeconds;
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
        this.keySerDe = new KeySerDe(rowKeyTypes);
        this.fileSchema = initialiseFileInfoSchema();
        this.partitionSchema = initialisePartitionSchema();
        this.conf = conf;
        this.regionSerDe = new RegionSerDe(this.tableSchema);
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        addFiles(Collections.singletonList(fileInfo));
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        for (FileInfo fileInfo : fileInfos) {
            if (null == fileInfo.getFilename()
                    || null == fileInfo.getFileStatus()
                    || null == fileInfo.getPartitionId()
                    || null == fileInfo.getNumberOfRecords()) {
                throw new IllegalArgumentException("FileInfo needs non-null filename, status, partition id and number of records: got " + fileInfo);
            }
        }
        Function<List<FileInfo>, List<FileInfo>> update = list -> {
            list.addAll(fileInfos);
            return list;
        };
        try {
            updateFiles(update);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(List<FileInfo> filesToBeMarkedReadyForGC, FileInfo newActiveFile)
            throws StateStoreException {
        Set<String> namesOfFilesToBeMarkedReadyForGC = filesToBeMarkedReadyForGC.stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toSet());

        Function<List<FileInfo>, String> condition = list -> {
            Map<String, FileInfo> fileNameToFileInfo = new HashMap<>();
            list.forEach(f -> fileNameToFileInfo.put(f.getFilename(), f));
            for (FileInfo fileInfo : filesToBeMarkedReadyForGC) {
                if (!fileNameToFileInfo.containsKey(fileInfo.getFilename())
                        || !fileNameToFileInfo.get(fileInfo.getFilename()).getFileStatus().equals(FileInfo.FileStatus.ACTIVE)) {
                    return "Files in filesToBeMarkedReadyForGC should be active: file " + fileInfo.getFilename() + " is not active";
                }
            }
            return "";
        };

        Function<List<FileInfo>, List<FileInfo>> update = list -> {
            List<FileInfo> filteredFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                if (namesOfFilesToBeMarkedReadyForGC.contains(fileInfo.getFilename())) {
                    fileInfo.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
                    fileInfo.setLastStateStoreUpdateTime(System.currentTimeMillis());
                }
                filteredFiles.add(fileInfo);
            }
            filteredFiles.add(newActiveFile);
            return filteredFiles;
        };

        try {
            updateFiles(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC,
                                                                         FileInfo leftFileInfo,
                                                                         FileInfo rightFileInfo) throws StateStoreException {
        Set<String> namesOfFilesToBeMarkedReadyForGC = new HashSet<>();
        filesToBeMarkedReadyForGC.stream().map(FileInfo::getFilename).forEach(namesOfFilesToBeMarkedReadyForGC::add);

        Function<List<FileInfo>, String> condition = list -> {
            Map<String, FileInfo> fileNameToFileInfo = new HashMap<>();
            list.forEach(f -> fileNameToFileInfo.put(f.getFilename(), f));
            for (FileInfo fileInfo : filesToBeMarkedReadyForGC) {
                if (!fileNameToFileInfo.containsKey(fileInfo.getFilename())
                        || !fileNameToFileInfo.get(fileInfo.getFilename()).getFileStatus().equals(FileInfo.FileStatus.ACTIVE)) {
                    return "Files in filesToBeMarkedReadyForGC should be active: file " + fileInfo.getFilename() + " is not active";
                }
            }
            return "";
        };

        Function<List<FileInfo>, List<FileInfo>> update = list -> {
            List<FileInfo> filteredFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                if (namesOfFilesToBeMarkedReadyForGC.contains(fileInfo.getFilename())) {
                    fileInfo.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
                    fileInfo.setLastStateStoreUpdateTime(System.currentTimeMillis());
                }
                filteredFiles.add(fileInfo);
            }
            filteredFiles.add(leftFileInfo);
            filteredFiles.add(rightFileInfo);
            return filteredFiles;
        };
        try {
            updateFiles(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        Set<String> namesOfFiles = new HashSet<>();
        fileInfos.stream().map(FileInfo::getFilename).forEach(namesOfFiles::add);

        Function<List<FileInfo>, String> condition = list -> {
            Map<String, FileInfo> fileNameToFileInfo = new HashMap<>();
            list.forEach(f -> fileNameToFileInfo.put(f.getFilename(), f));
            for (FileInfo fileInfo : fileInfos) {
                if (!fileNameToFileInfo.containsKey(fileInfo.getFilename())
                        || null != fileNameToFileInfo.get(fileInfo.getFilename()).getJobId()) {
                    return "Files should have a null job status: file " + fileInfo.getFilename() + " doesn't meet this criteria";
                }
            }
            return "";
        };

        Function<List<FileInfo>, List<FileInfo>> update = list -> {
            List<FileInfo> filteredFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                if (namesOfFiles.contains(fileInfo.getFilename())) {
                    fileInfo.setJobId(jobId);
                }
                filteredFiles.add(fileInfo);
            }
            return filteredFiles;
        };

        try {
            updateFiles(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        } catch (StateStoreException e) {
            throw new StateStoreException("StateStoreException updating jobid of files");
        }
    }

    @Override
    public void deleteReadyForGCFile(FileInfo readyForGCFileInfo) throws StateStoreException {
        Function<List<FileInfo>, String> condition = list -> {
            Map<String, FileInfo> fileNameToFileInfo = new HashMap<>();
            list.forEach(f -> fileNameToFileInfo.put(f.getFilename(), f));

            FileInfo currentFileInfo = fileNameToFileInfo.get(readyForGCFileInfo.getFilename());
            if (!currentFileInfo.getFileStatus().equals(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)) {
                return "File to be deleted should be marked as ready for GC, got " + currentFileInfo.getFileStatus();
            }
            return "";
        };

        Function<List<FileInfo>, List<FileInfo>> update = list -> {
            List<FileInfo> filteredFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                if (!readyForGCFileInfo.getFilename().equals(fileInfo.getFilename())) {
                    filteredFiles.add(fileInfo);
                }
            }
            return filteredFiles;
        };

        try {
            updateFiles(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        RevisionId revisionId = getCurrentFilesRevisionId();
        if (null == revisionId) {
            return Collections.EMPTY_LIST;
        }
        try {
            List<FileInfo> fileInfos = readFileInfosFromParquet(getFilesPath(revisionId));
            return fileInfos.stream().filter(f -> f.getFileStatus().equals(FileInfo.FileStatus.ACTIVE)).collect(Collectors.toList());
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files", e);
        }
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        try {
            long delayInMilliseconds = 1000L * garbageCollectorDelayBeforeDeletionInSeconds;
            long deleteTime = System.currentTimeMillis() - delayInMilliseconds;
            List<FileInfo> fileInfos = readFileInfosFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            List<FileInfo> filesReadyForGC = fileInfos.stream().filter(f -> {
                if (!f.getFileStatus().equals(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)) {
                    return false;
                }
                long lastUpdateTime = f.getLastStateStoreUpdateTime();
                return lastUpdateTime < deleteTime;
            }).collect(Collectors.toList());
            return filesReadyForGC.iterator();
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving ready for GC files", e);
        }
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        try {
            List<FileInfo> fileInfos = readFileInfosFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            return fileInfos.stream().filter(f -> {
                if (!f.getFileStatus().equals(FileInfo.FileStatus.ACTIVE)) {
                    return false;
                }
                return null == f.getJobId();
            }).collect(Collectors.toList());
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files with no job id", e);
        }
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        List<FileInfo> files = getActiveFiles();
        Map<String, List<String>> partitionToFiles = new HashMap<>();
        for (FileInfo fileInfo : files) {
            String partition = fileInfo.getPartitionId();
            if (!partitionToFiles.containsKey(partition)) {
                partitionToFiles.put(partition, new ArrayList<>());
            }
            partitionToFiles.get(partition).add(fileInfo.getFilename());
        }
        return partitionToFiles;
    }

    private void updateFiles(Function<List<FileInfo>, List<FileInfo>> update) throws IOException, StateStoreException {
        updateFiles(update, l -> "");
    }

    private void updateFiles(Function<List<FileInfo>, List<FileInfo>> update, Function<List<FileInfo>, String> condition)
            throws IOException, StateStoreException {
        int numberAttempts = 0;
        while (numberAttempts < 10) {
            RevisionId revisionId = getCurrentFilesRevisionId();
            String filesPath = getFilesPath(revisionId);
            List<FileInfo> files;
            try {
                files = readFileInfosFromParquet(filesPath);
                LOGGER.debug("Attempt number {}: reading file information (revisionId = {}, path = {})",
                        numberAttempts, revisionId, filesPath);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to read file information; retrying");
                numberAttempts++;
                sleep(numberAttempts);
                continue;
            }

            // Check condition
            String conditionCheck = condition.apply(files);
            if (!conditionCheck.equals("")) {
                throw new StateStoreException("Conditional check failed: " + conditionCheck);
            }

            // Apply update
            List<FileInfo> updatedFiles = update.apply(files);
            LOGGER.debug("Applied update to file information");

            // Attempt to write update
            RevisionId nextRevisionId = getNextRevisionId(revisionId);
            String nextRevisionIdPath = getFilesPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated file information (revisionId = {}, path = {})",
                        nextRevisionId, nextRevisionIdPath);
                writeFileInfosToParquet(updatedFiles, nextRevisionIdPath);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to write file information; retrying");
                numberAttempts++;
                continue;
            }
            try {
                conditionalUpdateOfFileInfoRevisionId(revisionId, nextRevisionId);
                LOGGER.debug("Updated file information to revision {}", nextRevisionId);
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update files failed with conditional check failure, deleting file {} and retrying ({}) ",
                        numberAttempts, nextRevisionIdPath, e.getMessage());
                Path path = new Path(nextRevisionIdPath);
                path.getFileSystem(new Configuration()).delete(path, false);
                LOGGER.info("Deleted file {}", path);
                numberAttempts++;
                sleep(numberAttempts);
            }
        }
    }

    private void sleep(int n) {
        // Implements exponential back-off with jitter, see
        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        int sleepTimeInSeconds = (int) Math.min(120, Math.pow(2.0, n + 1));
        long sleepTimeWithJitter = (long) (Math.random() * sleepTimeInSeconds * 1000L);
        try {
            Thread.sleep(sleepTimeWithJitter);
        } catch (InterruptedException e) {
            // Do nothing
        }
    }

    private RevisionId getCurrentPartitionsRevisionId() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(REVISION_ID_KEY, new AttributeValue().withS(CURRENT_PARTITIONS_REVISION_ID_KEY));
        GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withKey(key);
        GetItemResult result = dynamoDB.getItem(getItemRequest);
        if (null == result || null == result.getItem() || result.getItem().isEmpty()) {
            return null;
        }
        Map<String, AttributeValue> map = result.getItem();
        String revision = map.get(CURRENT_REVISION).getS();
        String uuid = map.get(CURRENT_UUID).getS();
        return new RevisionId(revision, uuid);
    }

    private RevisionId getCurrentFilesRevisionId() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(REVISION_ID_KEY, new AttributeValue().withS(CURRENT_FILES_REVISION_ID_KEY));
        GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withKey(key);
        GetItemResult result = dynamoDB.getItem(getItemRequest);
        if (null == result || null == result.getItem() || result.getItem().isEmpty()) {
            return null;
        }
        Map<String, AttributeValue> map = result.getItem();
        String revision = map.get(CURRENT_REVISION).getS();
        String uuid = map.get(CURRENT_UUID).getS();
        return new RevisionId(revision, uuid);
    }

    private void conditionalUpdateOfPartitionRevisionId(RevisionId currentRevisionId, RevisionId newRevisionId) {
        LOGGER.debug("Attempting conditional update of partition information from revision id {} to {}", currentRevisionId, newRevisionId);
        conditionalUpdateOfRevisionId(CURRENT_PARTITIONS_REVISION_ID_KEY, currentRevisionId, newRevisionId);
    }

    private void conditionalUpdateOfFileInfoRevisionId(RevisionId currentRevisionId, RevisionId newRevisionId) {
        LOGGER.debug("Attempting conditional update of file information from revision id {} to {}", currentRevisionId, newRevisionId);
        conditionalUpdateOfRevisionId(CURRENT_FILES_REVISION_ID_KEY, currentRevisionId, newRevisionId);
    }

    private void conditionalUpdateOfRevisionId(String revisionIdValue, RevisionId currentRevisionId, RevisionId newRevisionId) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(REVISION_ID_KEY, new AttributeValue().withS(revisionIdValue));
        item.put(CURRENT_REVISION, new AttributeValue().withS(newRevisionId.getRevision()));
        item.put(CURRENT_UUID, new AttributeValue().withS(newRevisionId.getUuid()));

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":currentrevision", new AttributeValue(currentRevisionId.getRevision()));
        expressionAttributeValues.put(":currentuuid", new AttributeValue(currentRevisionId.getUuid()));
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withItem(item)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withConditionExpression(CURRENT_REVISION + " = :currentrevision and " + CURRENT_UUID + " = :currentuuid");
        dynamoDB.putItem(putItemRequest);
    }

    private RevisionId getNextRevisionId(RevisionId currentRevisionId) {
        String revision = currentRevisionId.getRevision();
        while (revision.startsWith("0")) {
            revision = revision.substring(1);
        }
        long revisionNumber = Long.parseLong(revision);
        long nextRevisionNumber = revisionNumber + 1;
        StringBuilder nextRevision = new StringBuilder("" + nextRevisionNumber);
        while (nextRevision.length() < 12) {
            nextRevision.insert(0, "0");
        }
        return new RevisionId(nextRevision.toString(), UUID.randomUUID().toString());
    }

    private static class RevisionId {
        private final String revision;
        private final String uuid;

        RevisionId(String revision, String uuid) {
            this.revision = revision;
            this.uuid = uuid;
        }

        public String getRevision() {
            return revision;
        }

        public String getUuid() {
            return uuid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RevisionId)) {
                return false;
            }
            RevisionId that = (RevisionId) o;
            return Objects.equals(revision, that.revision) && Objects.equals(uuid, that.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(revision, uuid);
        }

        @Override
        public String toString() {
            return "RevisionId{" +
                    "revision='" + revision + '\'' +
                    ", uuid='" + uuid + '\'' +
                    '}';
        }
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition,
                                                          Partition newPartition1,
                                                          Partition newPartition2) throws StateStoreException {
        int numberAttempts = 0;
        while (numberAttempts < 5) {
            RevisionId revisionId = getCurrentPartitionsRevisionId();
            String partitionsPath = getPartitionsPath(revisionId);
            Map<String, Partition> partitionIdToPartition;
            try {
                List<Partition> partitions = readPartitionsFromParquet(partitionsPath);
                LOGGER.debug("Attempt number {}: reading partition information (revisionId = {}, path = {})",
                        numberAttempts, revisionId, partitionsPath);
                partitionIdToPartition = getMapFromPartitionIdToPartition(partitions);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to read partition information; retrying");
                numberAttempts++;
                continue;
            }

            // Validate request
            validateSplitPartitionRequest(partitionIdToPartition, splitPartition, newPartition1, newPartition2);

            // Update map from partition id to partitions
            partitionIdToPartition.put(splitPartition.getId(), splitPartition);
            partitionIdToPartition.put(newPartition1.getId(), newPartition1);
            partitionIdToPartition.put(newPartition2.getId(), newPartition2);

            // Convert to list of partitions
            List<Partition> updatedPartitions = new ArrayList<>();
            for (Map.Entry<String, Partition> entry : partitionIdToPartition.entrySet()) {
                updatedPartitions.add(entry.getValue());
            }

            RevisionId nextRevisionId = getNextRevisionId(revisionId);
            String nextRevisionIdPath = getPartitionsPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated partition information (revisionId = {}, path = {})",
                        nextRevisionId, nextRevisionIdPath);
                writePartitionsToParquet(updatedPartitions, nextRevisionIdPath);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to write partition information; retrying");
                numberAttempts++;
                continue;
            }
            try {
                conditionalUpdateOfPartitionRevisionId(revisionId, nextRevisionId);
                LOGGER.debug("Updated partition {}, new revision id is {}", splitPartition, nextRevisionId);
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update partitions failed with conditional check failure, retrying ({})",
                        numberAttempts, e.getMessage());
                Path path = new Path(nextRevisionIdPath);
                try {
                    path.getFileSystem(new Configuration()).delete(path, false);
                    LOGGER.debug("Deleted file {}", path);
                } catch (IOException e2) {
                    LOGGER.debug("IOException attempting to delete file {}: {}", path, e.getMessage());
                    // Ignore as not essential to delete the file
                }
                numberAttempts++;
                try {
                    Thread.sleep((long) (Math.random() * 2000L));
                } catch (InterruptedException interruptedException) {
                    // Do nothing
                }
            }
        }
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        RevisionId revisionId = getCurrentPartitionsRevisionId();
        if (null == revisionId) {
            return Collections.EMPTY_LIST;
        }
        String path = getPartitionsPath(revisionId);
        try {
            return readPartitionsFromParquet(path);
        } catch (IOException e) {
            throw new StateStoreException("IOException reading all partitions from path " + path, e);
        }
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        // TODO Optimise by passing the leaf predicate down
        return getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
    }

    private void validateSplitPartitionRequest(Map<String, Partition> partitionIdToPartition,
                                               Partition splitPartition,
                                               Partition newPartition1,
                                               Partition newPartition2)
            throws StateStoreException {
        // Validate that splitPartition is there and is a leaf partition
        if (!partitionIdToPartition.containsKey(splitPartition.getId())) {
            throw new StateStoreException("splitPartition should be present");
        }
        if (!partitionIdToPartition.get(splitPartition.getId()).isLeafPartition()) {
            throw new StateStoreException("splitPartition should be a leaf partition");
        }

        // Validate that newPartition1 and newPartition2 are not already there
        if (partitionIdToPartition.containsKey(newPartition1.getId()) || partitionIdToPartition.containsKey(newPartition2.getId())) {
            throw new StateStoreException("newPartition1 and newPartition2 should not be present");
        }

        // Validate that the children of splitPartition are newPartition1 and newPartition2
        Set<String> splitPartitionChildrenIds = new HashSet<>(splitPartition.getChildPartitionIds());
        Set<String> newIds = new HashSet<>();
        newIds.add(newPartition1.getId());
        newIds.add(newPartition2.getId());
        if (!splitPartitionChildrenIds.equals(newIds)) {
            throw new StateStoreException("Children of splitPartition do not equal newPartition1 and new Partition2");
        }

        // Validate that the parent of newPartition1 and newPartition2 are correct
        if (!newPartition1.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition1 does not equal splitPartition");
        }
        if (!newPartition2.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition2 does not equal splitPartition");
        }

        // Validate that newPartition1 and newPartition2 are leaf partitions
        if (!newPartition1.isLeafPartition() || !newPartition2.isLeafPartition()) {
            throw new StateStoreException("newPartition1 and newPartition2 should be leaf partitions");
        }
    }

    private Schema initialiseFileInfoSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("fileName", new StringType()))
                .valueFields(
                        new Field("fileStatus", new StringType()),
                        new Field("partitionId", new StringType()),
                        new Field("lastStateStoreUpdateTime", new LongType()),
                        new Field("numberOfRecords", new LongType()),
                        new Field("jobId", new StringType()),
                        new Field("minRowKeys", new ByteArrayType()),
                        new Field("maxRowKeys", new ByteArrayType()))
                .build();
    }

    private Schema initialisePartitionSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("partitionId", new StringType()))
                .valueFields(
                        new Field("leafPartition", new StringType()),
                        new Field("parentPartitionId", new StringType()),
                        new Field("childPartitionIds", new ListType(new StringType())),
                        new Field("region", new StringType()),
                        new Field("dimension", new IntType()))
                .build();
    }

    private String getFilesPath(RevisionId revisionId) {
        return fs + s3Bucket + "/statestore/files/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-files.parquet";
    }

    private String getPartitionsPath(RevisionId revisionId) {
        return fs + s3Bucket + "/statestore/partitions/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-partitions.parquet";
    }

    private Record getRecordFromFileInfo(FileInfo fileInfo) throws IOException {
        Record record = new Record();
        record.put("fileName", fileInfo.getFilename());
        record.put("fileStatus", "" + fileInfo.getFileStatus());
        record.put("partitionId", fileInfo.getPartitionId());
        record.put("lastStateStoreUpdateTime", fileInfo.getLastStateStoreUpdateTime());
        record.put("numberOfRecords", fileInfo.getNumberOfRecords());
        if (null == fileInfo.getJobId()) {
            record.put("jobId", "null");
        } else {
            record.put("jobId", fileInfo.getJobId());
        }
        record.put("minRowKeys", keySerDe.serialise(fileInfo.getMinRowKey()));
        record.put("maxRowKeys", keySerDe.serialise(fileInfo.getMaxRowKey()));
        return record;
    }

    private FileInfo getFileInfoFromRecord(Record record) throws IOException {
        FileInfo fileInfo = new FileInfo();
        fileInfo.setFilename((String) record.get("fileName"));
        fileInfo.setFileStatus(FileInfo.FileStatus.valueOf((String) record.get("fileStatus")));
        fileInfo.setPartitionId((String) record.get("partitionId"));
        fileInfo.setLastStateStoreUpdateTime((long) record.get("lastStateStoreUpdateTime"));
        fileInfo.setNumberOfRecords((long) record.get("numberOfRecords"));
        String jobId = (String) record.get("jobId");
        if ("null".equals(jobId)) {
            fileInfo.setJobId(null);
        } else {
            fileInfo.setJobId(jobId);
        }
        fileInfo.setMinRowKey(keySerDe.deserialise((byte[]) record.get("minRowKeys")));
        fileInfo.setMaxRowKey(keySerDe.deserialise((byte[]) record.get("maxRowKeys")));
        fileInfo.setRowKeyTypes(rowKeyTypes);
        return fileInfo;
    }

    private void writeFileInfosToParquet(List<FileInfo> fileInfos, String path) throws IOException {
        ParquetWriter<Record> recordWriter = new ParquetRecordWriter.Builder(new Path(path), SchemaConverter.getSchema(fileSchema), fileSchema)
                .withConf(conf)
                .build();
        for (FileInfo fileInfo : fileInfos) {
            recordWriter.write(getRecordFromFileInfo(fileInfo));
        }
        recordWriter.close();
        LOGGER.debug("Wrote fileinfos to " + path);
    }

    private List<FileInfo> readFileInfosFromParquet(String path) throws IOException {
        List<FileInfo> fileInfos = new ArrayList<>();
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(path), fileSchema)
                .withConf(conf)
                .build();
        ParquetReaderIterator recordReader = new ParquetReaderIterator(reader);
        while (recordReader.hasNext()) {
            fileInfos.add(getFileInfoFromRecord(recordReader.next()));
        }
        recordReader.close();
        return fileInfos;
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

    private void writePartitionsToParquet(List<Partition> partitions, String path) throws IOException {
        ParquetWriter<Record> recordWriter = new ParquetRecordWriter.Builder(new Path(path), SchemaConverter.getSchema(partitionSchema), partitionSchema)
                .withConf(conf)
                .build();
        for (Partition partition : partitions) {
            recordWriter.write(getRecordFromPartition(partition));
        }
        recordWriter.close();
        LOGGER.debug("Wrote partitions to " + path);
    }

    private List<Partition> readPartitionsFromParquet(String path) throws IOException {
        List<Partition> partitions = new ArrayList<>();
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(path), partitionSchema)
                .withConf(conf)
                .build();
        ParquetReaderIterator recordReader = new ParquetReaderIterator(reader);
        while (recordReader.hasNext()) {
            partitions.add(getPartitionFromRecord(recordReader.next()));
        }
        recordReader.close();
        return partitions;
    }

    private Record getRecordFromPartition(Partition partition) throws IOException {
        Record record = new Record();
        record.put("partitionId", partition.getId());
        record.put("leafPartition", "" + partition.isLeafPartition()); // TODO Change to boolean once boolean is a supported type
        String parentPartitionId;
        if (null == partition.getParentPartitionId()) {
            parentPartitionId = "null";
        } else {
            parentPartitionId = partition.getParentPartitionId();
        }
        record.put("parentPartitionId", parentPartitionId);
        record.put("childPartitionIds", partition.getChildPartitionIds());
        record.put("region", regionSerDe.toJson(partition.getRegion()));
        record.put("dimension", partition.getDimension());
        return record;
    }

    private Partition getPartitionFromRecord(Record record) throws IOException {
        Partition.Builder partitionBuilder = Partition.builder()
                .id((String) record.get("partitionId"))
                .leafPartition(record.get("leafPartition").equals("true"))
                .rowKeyTypes(rowKeyTypes)
                .childPartitionIds((List<String>) record.get("childPartitionIds"))
                .region(regionSerDe.fromJson((String) record.get("region")))
                .dimension((int) record.get("dimension"));
        String parentPartitionId = (String) record.get("parentPartitionId");
        if (!"null".equals(parentPartitionId)) {
            partitionBuilder.parentPartitionId(parentPartitionId);
        }
        return partitionBuilder.build();
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
        setInitialFileInfos();
    }

    public void setInitialFileInfos() throws StateStoreException {
        RevisionId firstRevisionId = new RevisionId(getZeroPaddedLong(1L), UUID.randomUUID().toString());
        String path = getFilesPath(firstRevisionId);
        try {
            writeFileInfosToParquet(Collections.EMPTY_LIST, path);
            LOGGER.debug("Written initial empty file to {}", path);
        } catch (IOException e) {
            throw new StateStoreException("IOException writing files to file " + path, e);
        }
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(REVISION_ID_KEY, new AttributeValue().withS(CURRENT_FILES_REVISION_ID_KEY));
        item.put(CURRENT_REVISION, new AttributeValue().withS(firstRevisionId.getRevision()));
        item.put(CURRENT_UUID, new AttributeValue().withS(firstRevisionId.getUuid()));
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withItem(item);
        dynamoDB.putItem(putItemRequest);
        LOGGER.debug("Put item to DynamoDB (item = {}, table = {})", item, dynamoRevisionIdTable);
    }

    private void setPartitions(List<Partition> partitions) throws StateStoreException {
        // Validate that there is no current revision id
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(REVISION_ID_KEY, new AttributeValue().withS(CURRENT_PARTITIONS_REVISION_ID_KEY));
        GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withKey(item);
        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
        if (null != getItemResult.getItem()) {
            throw new StateStoreException("Dynamo should not contain current revision id; found " + item);
        }

        // Write partitions to file
        long version = 1L;
        String versionString = getZeroPaddedLong(version);
        RevisionId revisionId = new RevisionId(versionString, UUID.randomUUID().toString());
        String path = getPartitionsPath(revisionId);
        try {
            writePartitionsToParquet(partitions, path);
            LOGGER.debug("Written initial partitions file to {}", path);
        } catch (IOException e) {
            throw new StateStoreException("IOException writing partitions to file " + path, e);
        }

        // Update Dynamo
        item.put(CURRENT_REVISION, new AttributeValue().withS(revisionId.getRevision()));
        item.put(CURRENT_UUID, new AttributeValue().withS(revisionId.getUuid()));
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withItem(item);
        dynamoDB.putItem(putItemRequest);
        LOGGER.debug("Put item to DynamoDB (item = {}, table = {})", item, dynamoRevisionIdTable);
    }

    private String getZeroPaddedLong(long number) {
        StringBuilder versionString = new StringBuilder("" + number);
        while (versionString.length() < 12) {
            versionString.insert(0, "0");
        }
        return versionString.toString();
    }
}
