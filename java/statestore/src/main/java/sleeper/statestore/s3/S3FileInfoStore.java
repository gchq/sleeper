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

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoStore;
import sleeper.core.statestore.FileReferences;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.statestore.s3.S3RevisionUtils.RevisionId;
import static sleeper.statestore.s3.S3StateStore.FIRST_REVISION;

class S3FileInfoStore implements FileInfoStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileInfoStore.class);
    private static final Schema FILE_SCHEMA = initialiseFileInfoSchema();
    private final String stateStorePath;
    private final Configuration conf;
    private final S3RevisionUtils s3RevisionUtils;
    private Clock clock = Clock.systemUTC();

    private S3FileInfoStore(Builder builder) {
        this.stateStorePath = Objects.requireNonNull(builder.stateStorePath, "stateStorePath must not be null");
        this.conf = Objects.requireNonNull(builder.conf, "hadoopConfiguration must not be null");
        this.s3RevisionUtils = Objects.requireNonNull(builder.s3RevisionUtils, "s3RevisionUtils must not be null");
    }

    static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        addFiles(Collections.singletonList(fileInfo));
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        long updateTime = clock.millis();

        Function<List<S3FileInfo>, String> condition = list -> {
            for (S3FileInfo existingS3File : list) {
                for (FileInfo newFile : fileInfos) {
                    if (existingS3File.getFileInfo().getFilename().equals(newFile.getFilename())
                            && existingS3File.getFileInfo().getPartitionId().equals(newFile.getPartitionId())) {
                        return "File already in system: " + newFile;
                    }
                }
            }
            return "";
        };
        Function<List<S3FileInfo>, List<S3FileInfo>> update = list -> {
            fileInfos.stream().map(S3FileInfo::active)
                    .map(file -> file.withUpdateTime(updateTime))
                    .forEach(list::add);
            return list;
        };
        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
            String partitionId, List<String> filesToBeMarkedReadyForGC, List<FileInfo> newFiles) throws StateStoreException {
        long updateTime = clock.millis();
        Set<String> partitionAndNameToBeMarked = new HashSet<>();
        filesToBeMarkedReadyForGC.stream()
                .map(file -> partitionId + "|" + file)
                .forEach(partitionAndNameToBeMarked::add);

        Function<List<S3FileInfo>, String> condition = list -> {
            Map<String, S3FileInfo> fileByPartitionAndName = new HashMap<>();
            list.forEach(f -> fileByPartitionAndName.put(f.getPartitionId() + "|" + f.getFilename(), f));
            for (String filename : filesToBeMarkedReadyForGC) {
                String partitionAndName = partitionId + "|" + filename;
                if (!fileByPartitionAndName.containsKey(partitionAndName)
                        || fileByPartitionAndName.get(partitionAndName).getFileStatus() != S3FileInfo.FileStatus.ACTIVE) {
                    return "Files in filesToBeMarkedReadyForGC should be active: file " + filename + " is not active in partition " + partitionId;
                }
            }
            return "";
        };

        Function<List<S3FileInfo>, List<S3FileInfo>> update = list -> {
            List<S3FileInfo> filteredFiles = new ArrayList<>();
            for (S3FileInfo fileInfo : list) {
                if (partitionAndNameToBeMarked.contains(fileInfo.getPartitionId() + "|" + fileInfo.getFilename())) {
                    fileInfo = fileInfo.toReadyForGC(updateTime);
                }
                filteredFiles.add(fileInfo);
            }
            for (FileInfo newFile : newFiles) {
                filteredFiles.add(S3FileInfo.active(setLastUpdateTime(newFile, updateTime)));
            }
            return filteredFiles;
        };
        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        long updateTime = clock.millis();
        Set<String> namesOfFiles = new HashSet<>();
        fileInfos.stream().map(FileInfo::getFilename).forEach(namesOfFiles::add);

        Function<List<S3FileInfo>, String> condition = list -> {
            Map<String, S3FileInfo> fileNameToFileInfo = new HashMap<>();
            list.forEach(f -> fileNameToFileInfo.put(f.getFilename(), f));
            for (FileInfo fileInfo : fileInfos) {
                if (!fileNameToFileInfo.containsKey(fileInfo.getFilename())
                        || null != fileNameToFileInfo.get(fileInfo.getFilename()).getJobId()) {
                    return "Files should have a null job status: file " + fileInfo.getFilename() + " doesn't meet this criteria";
                }
            }
            return "";
        };

        Function<List<S3FileInfo>, List<S3FileInfo>> update = list -> {
            List<S3FileInfo> filteredFiles = new ArrayList<>();
            for (S3FileInfo fileInfo : list) {
                if (namesOfFiles.contains(fileInfo.getFilename())) {
                    fileInfo = fileInfo.withJobId(jobId, updateTime);
                }
                filteredFiles.add(fileInfo);
            }
            return filteredFiles;
        };

        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        } catch (StateStoreException e) {
            throw new StateStoreException("StateStoreException updating jobid of files");
        }
    }

    @Override
    public void deleteReadyForGCFile(String readyForGCFilename) throws StateStoreException {
        Function<List<S3FileInfo>, String> condition = list -> {
            List<S3FileInfo> references = list.stream()
                    .filter(file -> file.getFilename().equals(readyForGCFilename))
                    .collect(Collectors.toUnmodifiableList());
            if (references.isEmpty()) {
                return "File not found: " + readyForGCFilename;
            }
            return references.stream()
                    .filter(f -> f.getFileStatus() != S3FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                    .findAny().map(f -> "File to be deleted should be marked as ready for GC, found active file on partition " + f.getPartitionId())
                    .orElse("");
        };

        Function<List<S3FileInfo>, List<S3FileInfo>> update = list -> list.stream()
                .filter(file -> !file.getFilename().equals(readyForGCFilename))
                .collect(Collectors.toUnmodifiableList());

        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        RevisionId revisionId = getCurrentFilesRevisionId();
        if (null == revisionId) {
            return Collections.emptyList();
        }
        try {
            List<S3FileInfo> fileInfos = readS3FileInfosFromParquet(getFilesPath(revisionId));
            return fileInfos.stream()
                    .filter(f -> f.getFileStatus() == S3FileInfo.FileStatus.ACTIVE)
                    .map(S3FileInfo::getFileInfo)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files", e);
        }
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        try {
            List<S3FileInfo> fileInfos = readS3FileInfosFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            Map<String, List<S3FileInfo>> referencesByName = fileInfos.stream()
                    .collect(Collectors.groupingBy(S3FileInfo::getFilename));
            return referencesByName.entrySet().stream()
                    .filter(entry -> entry.getValue().stream().allMatch(file ->
                            file.getFileStatus() == S3FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION &&
                                    Instant.ofEpochMilli(file.getLastUpdateTime()).isBefore(maxUpdateTime)))
                    .map(Map.Entry::getKey).distinct();
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving ready for GC files", e);
        }
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        try {
            List<S3FileInfo> fileInfos = readS3FileInfosFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            return fileInfos.stream()
                    .filter(f -> f.getFileStatus() == S3FileInfo.FileStatus.ACTIVE && f.getJobId() == null)
                    .map(S3FileInfo::getFileInfo).collect(Collectors.toList());
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

    @Override
    public AllFileReferences getAllFileReferences() throws StateStoreException {
        try {
            List<S3FileInfo> fileInfos = readS3FileInfosFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            Map<String, List<S3FileInfo>> referencesByFilename = fileInfos.stream()
                    .collect(Collectors.groupingBy(S3FileInfo::getFilename));
            return new AllFileReferences(referencesByFilename.entrySet().stream()
                    .map(entry -> references(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toUnmodifiableSet()));
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving files", e);
        }
    }

    private static FileReferences references(String filename, List<S3FileInfo> references) {
        Instant lastUpdateTime = references.stream()
                .map(fileInfo -> Instant.ofEpochMilli(fileInfo.getLastUpdateTime()))
                .max(Comparator.comparing(Function.identity())).orElseThrow();
        return new FileReferences(filename, lastUpdateTime, references.stream()
                .filter(f -> f.getFileStatus() == S3FileInfo.FileStatus.ACTIVE)
                .map(S3FileInfo::getFileInfo)
                .collect(Collectors.toUnmodifiableList()));
    }

    private void updateS3Files(Function<List<S3FileInfo>, List<S3FileInfo>> update, Function<List<S3FileInfo>, String> condition)
            throws IOException, StateStoreException {
        int numberAttempts = 0;
        while (numberAttempts < 10) {
            RevisionId revisionId = getCurrentFilesRevisionId();
            String filesPath = getFilesPath(revisionId);
            List<S3FileInfo> files;
            try {
                files = readS3FileInfosFromParquet(filesPath);
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
            if (!conditionCheck.isEmpty()) {
                throw new StateStoreException("Conditional check failed: " + conditionCheck);
            }

            // Apply update
            List<S3FileInfo> updatedFiles = update.apply(files);
            LOGGER.debug("Applied update to file information");

            // Attempt to write update
            RevisionId nextRevisionId = s3RevisionUtils.getNextRevisionId(revisionId);
            String nextRevisionIdPath = getFilesPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated file information (revisionId = {}, path = {})",
                        nextRevisionId, nextRevisionIdPath);
                writeS3FileInfosToParquet(updatedFiles, nextRevisionIdPath);
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
                path.getFileSystem(conf).delete(path, false);
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


    private RevisionId getCurrentFilesRevisionId() {
        return s3RevisionUtils.getCurrentFilesRevisionId();
    }

    private void conditionalUpdateOfFileInfoRevisionId(RevisionId currentRevisionId, RevisionId newRevisionId) {
        s3RevisionUtils.conditionalUpdateOfFileInfoRevisionId(currentRevisionId, newRevisionId);
    }

    private static Schema initialiseFileInfoSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("fileName", new StringType()))
                .valueFields(
                        new Field("fileStatus", new StringType()),
                        new Field("partitionId", new StringType()),
                        new Field("lastStateStoreUpdateTime", new LongType()),
                        new Field("numberOfRecords", new LongType()),
                        new Field("jobId", new StringType()),
                        new Field("countApproximate", new StringType()),
                        new Field("onlyContainsDataForThisPartition", new StringType()))
                .build();
    }

    public void initialise() throws StateStoreException {
        RevisionId firstRevisionId = new RevisionId(FIRST_REVISION, UUID.randomUUID().toString());
        String path = getFilesPath(firstRevisionId);
        try {
            writeS3FileInfosToParquet(Collections.emptyList(), path);
            LOGGER.debug("Written initial empty file to {}", path);
        } catch (IOException e) {
            throw new StateStoreException("IOException writing files to file " + path, e);
        }
        s3RevisionUtils.saveFirstFilesRevision(firstRevisionId);
    }

    @Override
    public boolean hasNoFiles() {
        RevisionId revisionId = getCurrentFilesRevisionId();
        if (revisionId == null) {
            return true;
        }
        String path = getFilesPath(revisionId);
        try (ParquetReader<Record> reader = fileInfosReader(path)) {
            return reader.read() == null;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed loading files", e);
        }
    }

    @Override
    public void clearTable() {
        Path path = new Path(stateStorePath + "/files");
        try {
            path.getFileSystem(conf).delete(path, true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        s3RevisionUtils.deleteFilesRevision();
    }

    private String getFilesPath(RevisionId revisionId) {
        return stateStorePath + "/files/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-files.parquet";
    }

    private Record getRecordFromS3FileInfo(S3FileInfo s3FileInfo) {
        FileInfo fileInfo = s3FileInfo.getFileInfo();
        Record record = new Record();
        record.put("fileName", fileInfo.getFilename());
        record.put("fileStatus", "" + s3FileInfo.getFileStatus());
        record.put("partitionId", fileInfo.getPartitionId());
        record.put("lastStateStoreUpdateTime", fileInfo.getLastStateStoreUpdateTime());
        record.put("numberOfRecords", fileInfo.getNumberOfRecords());
        if (null == fileInfo.getJobId()) {
            record.put("jobId", "null");
        } else {
            record.put("jobId", fileInfo.getJobId());
        }
        record.put("countApproximate", String.valueOf(fileInfo.isCountApproximate()));
        record.put("onlyContainsDataForThisPartition", String.valueOf(fileInfo.onlyContainsDataForThisPartition()));
        return record;
    }

    private S3FileInfo getS3FileInfoFromRecord(Record record) {
        String jobId = (String) record.get("jobId");
        return S3FileInfo.builder()
                .fileInfo(FileInfo.wholeFile()
                        .filename((String) record.get("fileName"))
                        .partitionId((String) record.get("partitionId"))
                        .lastStateStoreUpdateTime((Long) record.get("lastStateStoreUpdateTime"))
                        .numberOfRecords((Long) record.get("numberOfRecords"))
                        .jobId("null".equals(jobId) ? null : jobId)
                        .countApproximate(record.get("countApproximate").equals("true"))
                        .onlyContainsDataForThisPartition(record.get("onlyContainsDataForThisPartition").equals("true"))
                        .build())
                .status(S3FileInfo.FileStatus.valueOf((String) record.get("fileStatus")))
                .build();
    }

    private void writeS3FileInfosToParquet(List<S3FileInfo> fileInfos, String path) throws IOException {
        ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), FILE_SCHEMA, conf);

        for (S3FileInfo fileInfo : fileInfos) {
            recordWriter.write(getRecordFromS3FileInfo(fileInfo));
        }
        recordWriter.close();
        LOGGER.debug("Wrote fileinfos to " + path);
    }

    private List<S3FileInfo> readS3FileInfosFromParquet(String path) throws IOException {
        List<S3FileInfo> fileInfos = new ArrayList<>();
        try (ParquetReader<Record> reader = fileInfosReader(path)) {
            ParquetReaderIterator recordReader = new ParquetReaderIterator(reader);
            while (recordReader.hasNext()) {
                fileInfos.add(getS3FileInfoFromRecord(recordReader.next()));
            }
        }
        return fileInfos;
    }

    private ParquetReader<Record> fileInfosReader(String path) throws IOException {
        return new ParquetRecordReader.Builder(new Path(path), FILE_SCHEMA)
                .withConf(conf)
                .build();
    }

    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private static FileInfo setLastUpdateTime(FileInfo fileInfo, long updateTime) {
        return fileInfo.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }

    static final class Builder {
        private String stateStorePath;
        private Configuration conf;
        private S3RevisionUtils s3RevisionUtils;

        private Builder() {
        }

        Builder stateStorePath(String stateStorePath) {
            this.stateStorePath = stateStorePath;
            return this;
        }

        Builder conf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        Builder s3RevisionUtils(S3RevisionUtils s3RevisionUtils) {
            this.s3RevisionUtils = s3RevisionUtils;
            return this;
        }

        S3FileInfoStore build() {
            return new S3FileInfoStore(this);
        }
    }
}
