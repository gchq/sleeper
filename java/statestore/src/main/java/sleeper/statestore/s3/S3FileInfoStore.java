/*
 * Copyright 2022-2023 Crown Copyright
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
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.key.KeySerDe;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfo.FileStatus;
import sleeper.statestore.FileInfoStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.s3.S3RevisionUtils.RevisionId;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
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
import java.util.stream.Stream;

import static sleeper.statestore.s3.S3RevisionUtils.RevisionId;
import static sleeper.statestore.s3.S3StateStore.CURRENT_REVISION;
import static sleeper.statestore.s3.S3StateStore.CURRENT_UUID;
import static sleeper.statestore.s3.S3StateStore.REVISION_ID_KEY;

public class S3FileInfoStore implements FileInfoStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileInfoStore.class);
    private static final Function<List<FileInfo>, String> UNCONDITIONAL = l -> "";
    private static final Function<List<FileInfo>, List<FileInfo>> IDENTITY_UPDATE = l -> l;
    public static final String CURRENT_FILES_REVISION_ID_KEY = "CURRENT_FILES_REVISION_ID_KEY";
    private final List<PrimitiveType> rowKeyTypes;
    private final double garbageCollectorDelayBeforeDeletionInMinutes;
    private final KeySerDe keySerDe;
    private final String fs;
    private final String s3Bucket;
    private final AmazonDynamoDB dynamoDB;
    private final String dynamoRevisionIdTable;
    private final Schema fileSchema;
    private final Configuration conf;
    private final S3RevisionUtils s3RevisionUtils;
    private Clock clock = Clock.systemUTC();

    private S3FileInfoStore(Builder builder) {
        this.fs = Objects.requireNonNull(builder.fs, "fs must not be null");
        this.s3Bucket = Objects.requireNonNull(builder.s3Bucket, "s3Bucket must not be null");
        this.dynamoRevisionIdTable = Objects.requireNonNull(builder.dynamoRevisionIdTable, "dynamoRevisionIdTable must not be null");
        this.rowKeyTypes = builder.rowKeyTypes;
        this.garbageCollectorDelayBeforeDeletionInMinutes = builder.garbageCollectorDelayBeforeDeletionInMinutes;
        this.dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        this.keySerDe = new KeySerDe(rowKeyTypes);
        this.fileSchema = initialiseFileInfoSchema();
        this.conf = builder.conf;
        this.s3RevisionUtils = new S3RevisionUtils(dynamoDB, dynamoRevisionIdTable);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        addFiles(Collections.singletonList(fileInfo));
    }

    // DONE
    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        for (FileInfo fileInfo : fileInfos) {
            if (null == fileInfo.getFilename()
                    || null == fileInfo.getPartitionId()
                    || null == fileInfo.getNumberOfRecords()) {
                throw new IllegalArgumentException("FileInfo needs non-null filename, partition id and number of records: got " + fileInfo);
            }
        }

        Function<List<FileInfo>, List<FileInfo>> fileInPartitionUpdate = list -> {
            List<FileInfo> updatedFileList = new ArrayList<>(list);
            fileInfos.stream()
                .forEach(fileInfo -> {
                    FileInfo fileInPartition = fileInfo.cloneWithStatus(FileStatus.FILE_IN_PARTITION);
                    updatedFileList.add(fileInPartition);
                });
            return updatedFileList;
        };

        Function<List<FileInfo>, List<FileInfo>> fileLifecycleUpdate = list -> {
            List<FileInfo> updatedFileList = new ArrayList<>(list);
            fileInfos.stream()
                .forEach(fileInfo -> {
                    FileInfo fileLifecycle = fileInfo.cloneWithStatus(FileStatus.ACTIVE);
                    updatedFileList.add(fileLifecycle);
                });
            return updatedFileList;
        };

        try {
            updateFiles(fileInPartitionUpdate, fileLifecycleUpdate, UNCONDITIONAL, UNCONDITIONAL);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    // @Override
    // public void setStatusToReadyForGarbageCollection(String filename) throws StateStoreException {
    //     setStatusToReadyForGarbageCollection(Collections.singletonList(filename));
    // }

    private Map<String, List<FileInfo>> convertToMapFromFilenameToFileInfos(List<FileInfo> list) {
        Map<String, List<FileInfo>> fileNameToFileInfos = new HashMap<>();
        list.forEach(f -> {
            if (!fileNameToFileInfos.containsKey(f.getFilename())) {
                fileNameToFileInfos.put(f.getFilename(), new ArrayList<>());
            }
            fileNameToFileInfos.get(f.getFilename()).add(f);
        });
        return fileNameToFileInfos;
    }

    // @Override
    // public void setStatusToReadyForGarbageCollection(List<String> filenames) throws StateStoreException {
    //     // There should be no file in partition records for any of these filenames
    //     Function<List<FileInfo>, String> fileInPartitionCondition = list -> {
    //         Map<String, List<FileInfo>> fileNameToFileInfos = convertToMapFromFilenameToFileInfos(list);
    //         for (String filename : filenames) {
    //             if (fileNameToFileInfos.keySet().contains(filename)) {
    //                 return "Cannot set status of file " + filename
    //                     + " to READY_FOR_GARBAGE_COLLECTION as there exists a FILE_IN_PARTITION record for the file";
    //             }
    //         }
    //         return "";
    //     };

    //     // There must be a file lifecycle record for each of these filenames
    //     Function<List<FileInfo>, String> fileLifecycleCondition = list -> {
    //         Map<String, List<FileInfo>> fileNameToFileInfos = convertToMapFromFilenameToFileInfos(list);
    //         for (String filename : filenames) {
    //             if (!fileNameToFileInfos.keySet().contains(filename)) {
    //                 return "Cannot set status of file " + filename
    //                     + " to READY_FOR_GARBAGE_COLLECTION as there is no file lifecycle record for the file";
    //             }
    //         }
    //         return "";
    //     };

    //     // Update the file lifecyle record of each filename to READY_FOR_GARBAGE_COLLECTION and update the
    //     // lastStateStoreUpdateTime to now.
    //     Function<List<FileInfo>, List<FileInfo>> fileLifecycleUpdate = list -> {
    //         List<FileInfo> updatedFiles = new ArrayList<>();
    //         for (FileInfo fileInfo : list) {
    //             if (!filenames.contains(fileInfo.getFilename())) {
    //                 updatedFiles.add(fileInfo);
    //             }
    //         }
    //         Map<String, List<FileInfo>> fileNameToFileInfos = convertToMapFromFilenameToFileInfos(list);
    //         for (String filename : filenames) {
    //             FileInfo fileInfoWithGC = fileNameToFileInfos.get(filename).get(0)
    //                 .toBuilder()
    //                 .fileStatus(FileStatus.READY_FOR_GARBAGE_COLLECTION)
    //                 .lastStateStoreUpdateTime(Instant.now())
    //                 .build();
    //             updatedFiles.add(fileInfoWithGC);
    //         }
    //         return updatedFiles;
    //     };

    //     try {
    //         updateFiles(IDENTITY_UPDATE, fileLifecycleUpdate, fileInPartitionCondition, fileLifecycleCondition);
    //     } catch (IOException e) {
    //         throw new StateStoreException("IOException updating file infos", e);
    //     }
    // }

    @Override
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(
            List<FileInfo> fileInPartitionRecordsToBeDeleted, FileInfo newActiveFile) throws StateStoreException {
        Set<String> filenamesOfFileInPartitionRecordsToBeDeleted = fileInPartitionRecordsToBeDeleted
                .stream().map(FileInfo::getFilename).collect(Collectors.toSet());

        // Check that file-in-partition records exist for all the fileInPartitionRecordsToBeDeleted
        Function<List<FileInfo>, String> fileInPartitionCondition = list -> {
            Map<String, List<FileInfo>> fileNameToFileInfos = convertToMapFromFilenameToFileInfos(list);
            for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
                if (!fileNameToFileInfos.keySet().contains(fileInfo.getFilename())) {
                    return "Cannot remove file in partition record for file " + fileInfo.getFilename()
                        + " as there does not exist a FILE_IN_PARTITION record for the file";
                }
            }
            return "";
        };

        // Update file-in-partition records by deleting the files from fileInPartitionRecordsToBeDeleted
        Function<List<FileInfo>, List<FileInfo>> fileInPartitionUpdate = list -> {
            List<FileInfo> updatedFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                if (!filenamesOfFileInPartitionRecordsToBeDeleted.contains(fileInfo.getFilename())) {
                    updatedFiles.add(fileInfo);
                }
            }
            updatedFiles.add(newActiveFile.cloneWithStatus(FileInfo.FileStatus.FILE_IN_PARTITION));
            return updatedFiles;
        };

        // Add a file-lifecyle record for newActiveFile
        Function<List<FileInfo>, List<FileInfo>> fileLifeCycleUpdate = list -> {
            List<FileInfo> updatedFiles = new ArrayList<>(list);
            updatedFiles.add(newActiveFile.cloneWithStatus(FileInfo.FileStatus.ACTIVE));
            return updatedFiles;
        };

        try {
            updateFiles(fileInPartitionUpdate, fileLifeCycleUpdate, fileInPartitionCondition, UNCONDITIONAL);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(List<FileInfo> fileInPartitionRecordsToBeDeleted,
                                                                              FileInfo leftFileInfo,
                                                                              FileInfo rightFileInfo) throws StateStoreException {
        Set<String> filenamesOfFileInPartitionRecordsToBeDeleted = fileInPartitionRecordsToBeDeleted
            .stream().map(FileInfo::getFilename).collect(Collectors.toSet());

        // Check that file-in-partition records exist for all the fileInPartitionRecordsToBeDeleted
        Function<List<FileInfo>, String> fileInPartitionCondition = list -> {
            Map<String, List<FileInfo>> fileNameToFileInfos = convertToMapFromFilenameToFileInfos(list);
            for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
                if (!fileNameToFileInfos.keySet().contains(fileInfo.getFilename())) {
                    return "Cannot remove file in partition record for file " + fileInfo.getFilename()
                        + " as there does not exist a FILE_IN_PARTITION record for the file";
                }
            }
            return "";
        };

        // Update file-in-partition records by deleting the files from fileInPartitionRecordsToBeDeleted
        Function<List<FileInfo>, List<FileInfo>> fileInPartitionUpdate = list -> {
            List<FileInfo> updatedFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                if (!filenamesOfFileInPartitionRecordsToBeDeleted.contains(fileInfo.getFilename())) {
                    updatedFiles.add(fileInfo);
                }
            }
            updatedFiles.add(leftFileInfo.cloneWithStatus(FileInfo.FileStatus.FILE_IN_PARTITION));
            updatedFiles.add(rightFileInfo.cloneWithStatus(FileInfo.FileStatus.FILE_IN_PARTITION));
            return updatedFiles;
        };

        // Add a file-lifecyle record for newActiveFile
        Function<List<FileInfo>, List<FileInfo>> fileLifeCycleUpdate = list -> {
            List<FileInfo> updatedFiles = new ArrayList<>(list);
            updatedFiles.add(leftFileInfo.cloneWithStatus(FileInfo.FileStatus.ACTIVE));
            updatedFiles.add(rightFileInfo.cloneWithStatus(FileInfo.FileStatus.ACTIVE));
            return updatedFiles;
        };

        try {
            updateFiles(fileInPartitionUpdate, fileLifeCycleUpdate, fileInPartitionCondition, UNCONDITIONAL);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfosToBeUpdated) throws StateStoreException {
        Set<String> namesOfFiles = new HashSet<>();
        fileInfosToBeUpdated.stream().map(FileInfo::getFilename).forEach(namesOfFiles::add);

        Function<List<FileInfo>, String> fileInPartitionCondition = list -> {
            Map<String, List<FileInfo>> fileNameToFileInfos = convertToMapFromFilenameToFileInfos(list);
            for (FileInfo fileInfoToBeUpdated : fileInfosToBeUpdated) {
                if (!fileNameToFileInfos.containsKey(fileInfoToBeUpdated.getFilename())) {
                    return "There should be a file-in-partition record for the file " + fileInfoToBeUpdated.getFilename();
                }
                List<FileInfo> fileInPartitionRecords = fileNameToFileInfos.get(fileInfoToBeUpdated.getFilename());
                // Find record for this partition
                String partitionId = fileInfoToBeUpdated.getPartitionId();
                boolean foundFileInPartitionRecord = false;
                FileInfo fileInfoForThisPartition = null;
                for (FileInfo fileInfo : fileInPartitionRecords) {
                    if (partitionId.equals(fileInfo.getPartitionId())) {
                        foundFileInPartitionRecord = true;
                        fileInfoForThisPartition = fileInfo;
                    }
                }
                if (!foundFileInPartitionRecord) {
                    return "There should be a file-in-partition record for the file " + fileInfoToBeUpdated.getFilename()
                        + " for the partition " + partitionId;
                }
                if (null != fileInfoForThisPartition.getJobId()) {
                    return "There is a file-in-partition record for this file with a job id of " + fileInfoForThisPartition.getJobId();
                }
            }
            return "";
        };

        Function<List<FileInfo>, List<FileInfo>> fileInPartitionUpdate = list -> {
            Map<String, Map<String, FileInfo>> filenameToPartitionToFileInfo = new HashMap<>();
            for (FileInfo fileInfo : list) {
                filenameToPartitionToFileInfo.putIfAbsent(fileInfo.getFilename(), new HashMap<>());
                filenameToPartitionToFileInfo.get(fileInfo.getFilename()).put(fileInfo.getPartitionId(), fileInfo);
            }

            for (FileInfo fileInfo : fileInfosToBeUpdated) {
                FileInfo updatedFileInfo = filenameToPartitionToFileInfo
                    .get(fileInfo.getFilename())
                    .get(fileInfo.getPartitionId())
                    .toBuilder()
                    .jobId(jobId)
                    .build();
                filenameToPartitionToFileInfo
                    .get(fileInfo.getFilename())
                    .put(fileInfo.getPartitionId(), updatedFileInfo);
            }

            List<FileInfo> updatedFiles = new ArrayList<>();
            for (Map.Entry<String, Map<String, FileInfo>> entry : filenameToPartitionToFileInfo.entrySet()) {
                for (Map.Entry<String, FileInfo> entry2 : entry.getValue().entrySet()) {
                    updatedFiles.add(entry2.getValue());
                }
            }
            return updatedFiles;
        };

        try {
            updateFiles(fileInPartitionUpdate, IDENTITY_UPDATE, fileInPartitionCondition, UNCONDITIONAL);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        } catch (StateStoreException e) {
            throw new StateStoreException("StateStoreException updating jobid of files");
        }
    }

    @Override
    public void deleteFileLifecycleEntries(List<String> filenames) throws StateStoreException {
        Set<String> filenamesToDelete = new HashSet<>(filenames);

        Function<List<FileInfo>, List<FileInfo>> fileLifecycleUpdate = list -> {
            List<FileInfo> updatedFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                if (!filenamesToDelete.contains(fileInfo.getFilename())) {
                    updatedFiles.add(fileInfo);
                }
            }
            return updatedFiles;
        };

        try {
            updateFiles(IDENTITY_UPDATE, fileLifecycleUpdate, UNCONDITIONAL, UNCONDITIONAL);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public List<FileInfo> getFileInPartitionList() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        RevisionId revisionId = getCurrentFilesRevisionId();
        if (null == revisionId) {
            return Collections.EMPTY_LIST;
        }
        try {
            return readFileInfosFromParquet(getFileInPartitionsPath(revisionId));
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files", e);
        }
    }

    @Override
    public List<FileInfo> getFileLifecycleList() throws StateStoreException {
        RevisionId revisionId = getCurrentFilesRevisionId();
        if (null == revisionId) {
            return Collections.EMPTY_LIST;
        }
        try {
            return readFileInfosFromParquet(getFileLifecyclePath(revisionId));
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files", e);
        }
    }

    @Override
    public List<FileInfo> getActiveFileList() throws StateStoreException {
        return getFileLifecycleList().stream()
            .filter(f -> f.getFileStatus().equals(FileInfo.FileStatus.ACTIVE))
            .collect(Collectors.toList());
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFileInfos() throws StateStoreException {
        return getReadyForGCFileInfosStream().iterator();
    }

    @Override
    public Iterator<String> getReadyForGCFiles() throws StateStoreException {
        return getReadyForGCFileInfosStream().map(FileInfo::getFilename).iterator();
    }

    private Stream<FileInfo> getReadyForGCFileInfosStream() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        try {
            long delayInMilliseconds = (long) (1000.0 * 60.0 * garbageCollectorDelayBeforeDeletionInMinutes);
            long deleteTime = clock.millis() - delayInMilliseconds;
            List<FileInfo> fileInfos = readFileInfosFromParquet(getFileLifecyclePath(getCurrentFilesRevisionId()));
            return fileInfos.stream().filter(f -> {
                if (!f.getFileStatus().equals(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)) {
                    return false;
                }
                long lastUpdateTime = f.getLastStateStoreUpdateTime();
                return lastUpdateTime < deleteTime;
            });
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving ready for GC files", e);
        }
    }

    @Override
    public void findFilesThatShouldHaveStatusOfGCPending() throws StateStoreException {
        // List files from file-lifecycle table
        List<FileInfo> fileLifecycleList = getFileLifecycleList();
        Set<String> filenamesFromFileLifecycleList = fileLifecycleList.stream()
            .map(FileInfo::getFilename)
            .collect(Collectors.toSet());
        LOGGER.info("Found {} files in the file-lifecycle table", filenamesFromFileLifecycleList.size());

        // List files from file-in-partition table
        List<FileInfo> fileInPartitionList = getFileInPartitionList();
        Set<String> filenamesFromFileInPartitionList = fileInPartitionList.stream()
            .map(FileInfo::getFilename)
            .collect(Collectors.toSet());
        LOGGER.info("Found {} files in the file-in-partition table", filenamesFromFileInPartitionList.size());

        // Find any files which have a file-lifecycle entry but no file-in-partition entry
        filenamesFromFileLifecycleList.removeAll(filenamesFromFileInPartitionList);
        LOGGER.info("Found {} files which have file-lifecyle entries but no file-in-partition entries", filenamesFromFileLifecycleList.size());

        changeStatusOfFileLifecycleEntriesToGCPending(filenamesFromFileLifecycleList);
        LOGGER.info("Changed status of {} files in file-lifecyle table to GARBAGE_COLLECTION_PENDING", filenamesFromFileLifecycleList.size());
    }

    private void changeStatusOfFileLifecycleEntriesToGCPending(Set<String> filenames) throws StateStoreException {
        if (null == filenames || filenames.isEmpty()) {
            return;
        }

        // Update the file-lifecyle records for files in filenames
        Function<List<FileInfo>, List<FileInfo>> fileLifeCycleUpdate = list -> {
            List<FileInfo> updatedFiles = new ArrayList<>();
            for (FileInfo fileInfo : list) {
                String filename = fileInfo.getFilename();
                if (filenames.contains(filename)) {
                    updatedFiles.add(fileInfo.cloneWithStatus(FileStatus.GARBAGE_COLLECTION_PENDING));
                } else {
                    updatedFiles.add(fileInfo);
                }
            }
            return updatedFiles;
        };

        try {
            updateFiles(IDENTITY_UPDATE, fileLifeCycleUpdate, UNCONDITIONAL, UNCONDITIONAL);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file statuses to GARBAGE_COLLECTION_PENDING", e);
        }
    }

    @Override
    public List<FileInfo> getFileInPartitionInfosWithNoJobId() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        try {
            List<FileInfo> fileInfos = readFileInfosFromParquet(getFileInPartitionsPath(getCurrentFilesRevisionId()));
            return fileInfos.stream().filter(f -> null == f.getJobId()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files with no job id", e);
        }
    }

    @Override
    public Map<String, List<String>> getPartitionToFileInPartitionMap() throws StateStoreException {
        List<FileInfo> files = getFileInPartitionList();
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

    private void updateFiles(Function<List<FileInfo>, List<FileInfo>> updateToFileInPartitionRecords,
            Function<List<FileInfo>, List<FileInfo>> updateToFileLifecycleRecords,
            Function<List<FileInfo>, String> fileInPartitionCondition,
            Function<List<FileInfo>, String> fileLifecycleCondition)
            throws IOException, StateStoreException {
        int numberAttempts = 0;
        while (numberAttempts < 10) {
            RevisionId revisionId = getCurrentFilesRevisionId();
            String fileInPartitionsPath = getFileInPartitionsPath(revisionId);
            String fileLifecyclePath = getFileLifecyclePath(revisionId);
            List<FileInfo> fileInPartitions;
            List<FileInfo> fileLifecycles;
            try {
                fileInPartitions = readFileInfosFromParquet(fileInPartitionsPath);
                fileLifecycles = readFileInfosFromParquet(fileLifecyclePath);
                LOGGER.debug("Attempt number {}: reading file information (revisionId = {}, fileInPartitionsPath = {}, fileLifecyclePath = {})",
                        numberAttempts, revisionId, fileInPartitionsPath, fileLifecyclePath);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to read file information; retrying");
                numberAttempts++;
                sleep(numberAttempts);
                continue;
            }

            // Check conditions
            String fileInPartitionConditionCheck = fileInPartitionCondition.apply(fileInPartitions);
            if (!fileInPartitionConditionCheck.equals("")) {
                throw new StateStoreException("Conditional check of file in partition information failed: " + fileInPartitionConditionCheck);
            }
            String fileLifecycleConditionCheck = fileLifecycleCondition.apply(fileLifecycles);
            if (!fileLifecycleConditionCheck.equals("")) {
                throw new StateStoreException("Conditional check of file lifecycle information failed: " + fileLifecycleConditionCheck);
            }

            // Apply update
            List<FileInfo> updatedFileInPartitions = updateToFileInPartitionRecords.apply(fileInPartitions);
            List<FileInfo> updatedFileLifecycles = updateToFileLifecycleRecords.apply(fileLifecycles);
            LOGGER.debug("Applied update to file information");

            // Attempt to write update
            RevisionId nextRevisionId = s3RevisionUtils.getNextRevisionId(revisionId);
            String nextRevisionIdFileInPartitionPath = getFileInPartitionsPath(nextRevisionId);
            String nextRevisionIdFileLifecyclePath = getFileLifecyclePath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated file information (fileInPartitionsPath = {}, fileLifecyclePath = {})",
                    nextRevisionIdFileInPartitionPath, nextRevisionIdFileLifecyclePath);
                writeFileInfosToParquet(updatedFileInPartitions, nextRevisionIdFileInPartitionPath);
                writeFileInfosToParquet(updatedFileLifecycles, nextRevisionIdFileLifecyclePath);
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
                LOGGER.info("Attempt number {} to update files failed with conditional check failure, deleting files {}, {} and retrying ({}) ",
                        numberAttempts, nextRevisionIdFileInPartitionPath, nextRevisionIdFileLifecyclePath, e.getMessage());
                Path path = new Path(nextRevisionIdFileInPartitionPath);
                path.getFileSystem(new Configuration()).delete(path, false);
                LOGGER.info("Deleted file {}", nextRevisionIdFileInPartitionPath);
                path = new Path(nextRevisionIdFileLifecyclePath);
                path.getFileSystem(new Configuration()).delete(path, false);
                LOGGER.info("Deleted file {}", nextRevisionIdFileLifecyclePath);
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

    private RevisionId getCurrentFilesRevisionId() {
        return s3RevisionUtils.getCurrentFilesRevisionId();
    }

    private void conditionalUpdateOfFileInfoRevisionId(RevisionId currentRevisionId, RevisionId newRevisionId) {
        s3RevisionUtils.conditionalUpdateOfFileInfoRevisionId(currentRevisionId, newRevisionId);
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

    public void initialise() throws StateStoreException {
        RevisionId firstRevisionId = new RevisionId(S3StateStore.getZeroPaddedLong(1L), UUID.randomUUID().toString());
        String fileInPartitionsPath = getFileInPartitionsPath(firstRevisionId);
        String fileLifecyclePath = getFileLifecyclePath(firstRevisionId);
        try {
            writeFileInfosToParquet(Collections.EMPTY_LIST, fileInPartitionsPath);
            LOGGER.debug("Written initial empty file to {}", fileInPartitionsPath);
            writeFileInfosToParquet(Collections.EMPTY_LIST, fileLifecyclePath);
            LOGGER.debug("Written initial empty file to {}", fileLifecyclePath);
        } catch (IOException e) {
            throw new StateStoreException("IOException writing to files " + fileInPartitionsPath + ", " + fileLifecyclePath, e);
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

    private String getFileInPartitionsPath(RevisionId revisionId) {
        return fs + s3Bucket + "/statestore/files/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-file-in-partitions.parquet";
    }

    private String getFileLifecyclePath(RevisionId revisionId) {
        return fs + s3Bucket + "/statestore/files/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-file-lifecycle.parquet";
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
        String jobId = (String) record.get("jobId");
        return FileInfo.builder()
                .filename((String) record.get("fileName"))
                .fileStatus(FileInfo.FileStatus.valueOf((String) record.get("fileStatus")))
                .partitionId((String) record.get("partitionId"))
                .lastStateStoreUpdateTime((Long) record.get("lastStateStoreUpdateTime"))
                .numberOfRecords((Long) record.get("numberOfRecords"))
                .jobId("null".equals(jobId) ? null : jobId)
                .minRowKey(keySerDe.deserialise((byte[]) record.get("minRowKeys")))
                .maxRowKey(keySerDe.deserialise((byte[]) record.get("maxRowKeys")))
                .rowKeyTypes(rowKeyTypes)
                .build();
    }



    private void writeFileInfosToParquet(List<FileInfo> fileInfos, String path) throws IOException {
        ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), fileSchema, conf);

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

    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    public static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String dynamoRevisionIdTable;
        private List<PrimitiveType> rowKeyTypes;
        private String fs;
        private String s3Bucket;
        private double garbageCollectorDelayBeforeDeletionInMinutes;
        private Configuration conf;

        public Builder() {
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public Builder dynamoRevisionIdTable(String dynamoRevisionIdTable) {
            this.dynamoRevisionIdTable = dynamoRevisionIdTable;
            return this;
        }

        public Builder rowKeyTypes(List<PrimitiveType> rowKeyTypes) {
            this.rowKeyTypes = rowKeyTypes;
            return this;
        }

        public Builder fs(String fs) {
            this.fs = fs;
            return this;
        }

        public Builder s3Bucket(String s3Bucket) {
            this.s3Bucket = s3Bucket;
            return this;
        }

        public S3FileInfoStore build() {
            return new S3FileInfoStore(this);
        }

        public Builder garbageCollectorDelayBeforeDeletionInMinutes(double garbageCollectorDelayBeforeDeletionInMinutes) {
            this.garbageCollectorDelayBeforeDeletionInMinutes = garbageCollectorDelayBeforeDeletionInMinutes;
            return this;
        }

        public Builder conf(Configuration conf) {
            this.conf = conf;
            return this;
        }
    }
}
