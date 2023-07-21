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
package sleeper.statestore.inmemory;

import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoStore;
import sleeper.statestore.FileLifecycleInfo;
import sleeper.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static sleeper.statestore.FileLifecycleInfo.FileStatus.ACTIVE;
import static sleeper.statestore.FileLifecycleInfo.FileStatus.GARBAGE_COLLECTION_PENDING;;

/**
 * This class is intended for testing only. It is not thread-safe and should not be used
 * where concurrent operations may be happening.
 */
public class InMemoryFileInfoStore implements FileInfoStore {
    private final Map<String, Map<String, FileInfo>> fileInPartitionEntries = new HashMap<>(); // filename -> partition id -> fileinfo
    private final Map<String, FileLifecycleInfo> fileLifecycleEntries = new HashMap<>();
    private final long garbageCollectorDelayBeforeDeletionInSeconds;

    public InMemoryFileInfoStore(long garbageCollectorDelayBeforeDeletionInSeconds) {
        this.garbageCollectorDelayBeforeDeletionInSeconds = garbageCollectorDelayBeforeDeletionInSeconds;
    }

    public InMemoryFileInfoStore() {
        this(Long.MAX_VALUE);
    }

    @Override
    public void addFile(FileInfo fileInfo) {
        if (null == fileInfo.getFilename()
                || null == fileInfo.getPartitionId()
                || null == fileInfo.getNumberOfRecords()) {
            throw new IllegalArgumentException("FileInfo needs non-null filename, partition, number of records: got " + fileInfo);
        }
        fileInPartitionEntries.putIfAbsent(fileInfo.getFilename(), new HashMap<>());
        fileInPartitionEntries.get(fileInfo.getFilename()).put(fileInfo.getPartitionId(), fileInfo);
        fileLifecycleEntries.put(fileInfo.getFilename(), fileInfo.toFileLifecycleInfo(ACTIVE));
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) {
        fileInfos.stream().forEach(this::addFile);
    }

    @Override
    public synchronized void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(List<FileInfo> fileInPartitionRecordsToBeDeleted,
            FileInfo newActiveFile) throws StateStoreException {
        checkAndDeleteFileInPartitionInfos(fileInPartitionRecordsToBeDeleted);
        addFile(newActiveFile);
    }

    @Override
    public synchronized void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(List<FileInfo> fileInPartitionRecordsToBeDeleted,
            FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        checkAndDeleteFileInPartitionInfos(fileInPartitionRecordsToBeDeleted);
        addFile(leftFileInfo);
        addFile(rightFileInfo);
    }

    @Override
    public synchronized void atomicallySplitFileInPartitionRecord(FileInfo fileInPartitionRecordToBeSplit,
            String leftChildPartitionId, String rightChildPartitionId) throws StateStoreException {
        // Check that the fileInPartitionRecordToBeSplit record exists
        if (!fileInPartitionEntries.containsKey(fileInPartitionRecordToBeSplit.getFilename())) {
            throw new StateStoreException("There is no file-in-partition entry for filename " + fileInPartitionRecordToBeSplit.getFilename());
        }
        if (!fileInPartitionEntries.get(fileInPartitionRecordToBeSplit.getFilename())
            .containsKey(fileInPartitionRecordToBeSplit.getPartitionId())) {
            throw new StateStoreException("There is no file-in-partition entry for filename " + fileInPartitionRecordToBeSplit.getFilename()
                + " and partition " + fileInPartitionRecordToBeSplit.getPartitionId());
        }

        // Check that the existing FileInfo has a null job id
        FileInfo existingFileInfo = fileInPartitionEntries
            .get(fileInPartitionRecordToBeSplit.getFilename())
            .get(fileInPartitionRecordToBeSplit.getPartitionId());
        if (existingFileInfo.getJobId() != null) {
            throw new StateStoreException("The FileInfo for filename " + fileInPartitionRecordToBeSplit.getFilename()
                + " and partition " + fileInPartitionRecordToBeSplit.getPartitionId() + " has a job id");
        }

        // Create the two child ones
        FileInfo leftFileInfo = existingFileInfo.toBuilder()
            .partitionId(leftChildPartitionId)
            .onlyContainsDataForThisPartition(false)
            .build();
        FileInfo rightFileInfo = existingFileInfo.toBuilder()
            .partitionId(rightChildPartitionId)
            .onlyContainsDataForThisPartition(false)
            .build();

        // Remove file-in-partition record
        fileInPartitionEntries.get(fileInPartitionRecordToBeSplit.getFilename())
            .remove(fileInPartitionRecordToBeSplit.getPartitionId());

        // Add two new file-in-partition records
        fileInPartitionEntries.get(fileInPartitionRecordToBeSplit.getFilename())
            .put(leftChildPartitionId, leftFileInfo);
        fileInPartitionEntries.get(fileInPartitionRecordToBeSplit.getFilename())
            .put(rightChildPartitionId, rightFileInfo);
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        List<String> filenamesWithJobId = findFilenamesWithJobIdSet(fileInfos);
        if (!filenamesWithJobId.isEmpty()) {
            throw new StateStoreException("Job ID already set: " + filenamesWithJobId);
        }
        for (FileInfo file : fileInfos) {
            FileInfo temp = fileInPartitionEntries.get(file.getFilename()).get(file.getPartitionId()).toBuilder()
                .jobId(jobId)
                .build();
            fileInPartitionEntries.get(file.getFilename()).put(file.getPartitionId(), temp);
        }
    }

    private List<String> findFilenamesWithJobIdSet(List<FileInfo> fileInfos) {
        List<String> filenamesWithJobIdSet = new ArrayList<>();
        for (FileInfo file : fileInfos) {
            if (null != fileInPartitionEntries.get(file.getFilename()).get(file.getPartitionId()).getJobId()) {
                filenamesWithJobIdSet.add(file.getFilename());
            }
        }
        return filenamesWithJobIdSet;
    }

    @Override
    public void deleteFileLifecycleEntries(List<String> filenames) throws StateStoreException {
        filenames.stream().forEach(this::deleteReadyForGCFile);
    }

    private void deleteReadyForGCFile(String filename) {
        fileLifecycleEntries.remove(filename);
    }

    @Override
    public List<FileInfo> getFileInPartitionList() {
        return fileInPartitionEntries.values().stream()
            .map(map -> map.values())
            .flatMap(c -> c.stream())
            .collect(Collectors.toList());
    }

    @Override
    public List<FileLifecycleInfo> getFileLifecycleList() throws StateStoreException {
        return new ArrayList<>(fileLifecycleEntries.values());
    }

    @Override
    public List<FileLifecycleInfo> getActiveFileList() throws StateStoreException {
        return fileLifecycleEntries.values().stream()
            .filter(f -> f.getFileStatus().equals(ACTIVE))
            .collect(Collectors.toList());
    }

    @Override
    public Iterator<String> getReadyForGCFiles() {
        return getReadyForGCFileInfoStream().map(FileLifecycleInfo::getFilename).iterator();
    }

    @Override
    public Iterator<FileLifecycleInfo> getReadyForGCFileInfos() throws StateStoreException {
        return getReadyForGCFileInfoStream().iterator();
    }

    private Stream<FileLifecycleInfo> getReadyForGCFileInfoStream() {
        long delayInMilliseconds = 1000L * garbageCollectorDelayBeforeDeletionInSeconds;
        long deleteTime = System.currentTimeMillis() - delayInMilliseconds;
        return fileLifecycleEntries.values().stream()
            .filter(f -> f.getFileStatus().equals(GARBAGE_COLLECTION_PENDING))
            .filter(f -> (f.getLastStateStoreUpdateTime() < deleteTime));
    }

    @Override
    public void findFilesThatShouldHaveStatusOfGCPending() throws StateStoreException {
        // Identify any files which have lifecycle records that do not have any
        // file-in-partition records.
        Set<String> allFilesWithLifecycleEntries = new HashSet<>(fileLifecycleEntries.keySet());
        Set<String> allFilesWithFileInPartitionEntries = new HashSet<>(fileInPartitionEntries.keySet());
        allFilesWithLifecycleEntries.removeAll(allFilesWithFileInPartitionEntries);
        // Update the file-lifecylce records to GARBAGE_COLLECTION_PENDING.
        for (String filename : allFilesWithLifecycleEntries) {
            FileLifecycleInfo updatedFileInfo = fileLifecycleEntries.get(filename).cloneWithStatus(GARBAGE_COLLECTION_PENDING);
            fileLifecycleEntries.put(filename, updatedFileInfo);
        }
    }

    @Override
    public List<FileInfo> getFileInPartitionInfosWithNoJobId() {
        return getFileInPartitionList().stream()
                .filter(file -> file.getJobId() == null)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, List<String>> getPartitionToFileInPartitionMap() {
        return getFileInPartitionList().stream().collect(
                groupingBy(FileInfo::getPartitionId,
                        mapping(FileInfo::getFilename, Collectors.toList())));
    }

    private synchronized void checkAndDeleteFileInPartitionInfos(List<FileInfo> fileInPartitionRecordsToBeDeleted)
            throws StateStoreException {
        // Check that the right file-in-partition records exist
        List<FileInfo> failures = new ArrayList<>();
        for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
            if (!fileInPartitionEntries.containsKey(fileInfo.getFilename())) {
                failures.add(fileInfo);
            } else {
                Map<String, FileInfo> partitionToFileInfo = fileInPartitionEntries.get(fileInfo.getFilename());
                if (!partitionToFileInfo.containsKey(fileInfo.getPartitionId())) {
                    failures.add(fileInfo);
                }
            }
        }
        if (!failures.isEmpty()) {
            throw new StateStoreException("Some of the provided fileInPartitionRecordsToBeDeleted do not have the correct"
                + " file-in-partition entries" + failures);
        }

        // Delete the records
        for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
            fileInPartitionEntries.get(fileInfo.getFilename()).remove(fileInfo.getPartitionId());
            if (fileInPartitionEntries.get(fileInfo.getFilename()).isEmpty()) {
                fileInPartitionEntries.remove(fileInfo.getFilename());
            }
        }
    }

    @Override
    public void initialise() throws StateStoreException {
    }
}
