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
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoSerDe;
import sleeper.core.statestore.FileInfoStore;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.statestore.s3.S3RevisionUtils.RevisionId;
import static sleeper.statestore.s3.S3StateStore.FIRST_REVISION;

class S3FileInfoStore implements FileInfoStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileInfoStore.class);
    private static final Schema FILE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("fileName", new StringType()))
            .valueFields(
                    new Field("referencesJson", new StringType()),
                    new Field("externalReferences", new IntType()),
                    new Field("lastStateStoreUpdateTime", new LongType()))
            .build();

    private final String stateStorePath;
    private final Configuration conf;
    private final S3RevisionUtils s3RevisionUtils;
    private final FileInfoSerDe serDe = new FileInfoSerDe();
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
        Instant updateTime = clock.instant();
        Map<String, FileInfo> newFilesByPartitionAndFilename = fileInfos.stream()
                .collect(Collectors.toMap(
                        fileInfo -> fileInfo.getPartitionId() + "|" + fileInfo.getFilename(),
                        Function.identity()));
        Function<List<S3FileInfo>, String> condition = list -> list.stream()
                .flatMap(file -> file.getInternalReferences().stream())
                .map(existingFile -> {
                    String partitionIdAndName = existingFile.getPartitionId() + "|" + existingFile.getFilename();
                    if (newFilesByPartitionAndFilename.containsKey(partitionIdAndName)) {
                        return "File already in system: " + newFilesByPartitionAndFilename.get(partitionIdAndName);
                    }
                    return null;
                }).filter(Objects::nonNull)
                .findFirst().orElse("");
        Function<List<S3FileInfo>, List<S3FileInfo>> update = list -> {
            list.addAll(S3FileInfo.fromFileReferences(fileInfos, updateTime));
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
        Instant updateTime = clock.instant();
        Set<String> filesToBeMarkedReadyForGCSet = new HashSet<>(filesToBeMarkedReadyForGC);

        Function<List<S3FileInfo>, String> condition = list -> {
            Set<String> activePartitionFiles = new HashSet<>();
            for (S3FileInfo existingFile : list) {
                for (FileInfo reference : existingFile.getInternalReferences()) {
                    activePartitionFiles.add(reference.getPartitionId() + "|" + reference.getFilename());
                }
            }
            for (String filename : filesToBeMarkedReadyForGC) {
                if (!activePartitionFiles.contains(partitionId + "|" + filename)) {
                    return "Files in filesToBeMarkedReadyForGC should be active: file " + filename + " is not active in partition " + partitionId;
                }
            }
            return "";
        };

        Function<List<S3FileInfo>, List<S3FileInfo>> update = list -> {
            List<S3FileInfo> newS3Files = S3FileInfo.fromFileReferences(newFiles, updateTime);
            Map<String, S3FileInfo> newFilesByName = newS3Files.stream()
                    .collect(Collectors.toMap(S3FileInfo::getFilename, Function.identity()));
            List<S3FileInfo> after = new ArrayList<>();
            Set<String> filenamesWithUpdatedReferences = new HashSet<>();
            for (S3FileInfo existingFile : list) {
                S3FileInfo file = existingFile;
                if (filesToBeMarkedReadyForGCSet.contains(existingFile.getFilename())) {
                    file = file.withoutReferenceForPartition(partitionId, updateTime);
                }
                S3FileInfo newFile = newFilesByName.get(existingFile.getFilename());
                if (newFile != null) {
                    file = file.withUpdatedReferences(newFile);
                    filenamesWithUpdatedReferences.add(existingFile.getFilename());
                }
                after.add(file);
            }
            return Stream.concat(
                            after.stream(),
                            newS3Files.stream().filter(file -> !filenamesWithUpdatedReferences.contains(file.getFilename())))
                    .collect(Collectors.toUnmodifiableList());
        };
        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file infos", e);
        }
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        Instant updateTime = clock.instant();
        Set<String> partitionAndNames = fileInfos.stream()
                .map(f -> f.getPartitionId() + "|" + f.getFilename())
                .collect(Collectors.toSet());
        Map<String, Set<String>> partitionUpdatesByName = fileInfos.stream()
                .collect(Collectors.groupingBy(FileInfo::getFilename,
                        Collectors.mapping(FileInfo::getPartitionId, Collectors.toUnmodifiableSet())));

        Function<List<S3FileInfo>, String> condition = list -> {
            Set<String> missing = new HashSet<>(partitionAndNames);
            for (S3FileInfo existing : list) {
                for (FileInfo reference : existing.getInternalReferences()) {
                    String partitionAndName = reference.getPartitionId() + "|" + reference.getFilename();
                    if (missing.remove(partitionAndName) && reference.getJobId() != null) {
                        return "Job already assigned for partition|filename: " + partitionAndName;
                    }
                }
            }
            if (!missing.isEmpty()) {
                return "Files not found with partition|filename: " + missing;
            }
            return "";
        };

        Function<List<S3FileInfo>, List<S3FileInfo>> update = list -> {
            List<S3FileInfo> filteredFiles = new ArrayList<>();
            for (S3FileInfo existing : list) {
                Set<String> partitionUpdates = partitionUpdatesByName.get(existing.getFilename());
                if (partitionUpdates == null) {
                    filteredFiles.add(existing);
                } else {
                    filteredFiles.add(existing.withJobIdForPartitions(jobId, partitionUpdates, updateTime));
                }
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
            Optional<S3FileInfo> existingFile = list.stream()
                    .filter(file -> file.getFilename().equals(readyForGCFilename))
                    .findFirst();
            if (existingFile.isEmpty()) {
                return "File not found: " + readyForGCFilename;
            }
            S3FileInfo file = existingFile.get();
            if (file.getReferenceCount() > 0) {
                return "File to be deleted should be marked as ready for GC, found reference count " + file.getReferenceCount();
            } else {
                return "";
            }
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
                    .flatMap(file -> file.getInternalReferences().stream())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files", e);
        }
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        try {
            List<S3FileInfo> files = readS3FileInfosFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            return files.stream()
                    .filter(file -> file.getReferenceCount() == 0 && file.getLastUpdateTime().isBefore(maxUpdateTime))
                    .map(S3FileInfo::getFilename).distinct();
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
                    .flatMap(file -> file.getInternalReferences().stream())
                    .filter(f -> f.getJobId() == null)
                    .collect(Collectors.toList());
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
    public AllFileReferences getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        try {
            List<S3FileInfo> files = readS3FileInfosFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            Set<FileInfo> activeFiles = files.stream()
                    .flatMap(file -> file.getInternalReferences().stream())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            List<String> filesWithNoReferences = files.stream()
                    .filter(file -> file.getReferenceCount() == 0)
                    .map(S3FileInfo::getFilename)
                    .collect(Collectors.toUnmodifiableList());
            boolean moreThanMax = filesWithNoReferences.size() > maxUnreferencedFiles;
            if (moreThanMax) {
                filesWithNoReferences = filesWithNoReferences.subList(0, maxUnreferencedFiles);
            }
            return new AllFileReferences(activeFiles, new TreeSet<>(filesWithNoReferences), moreThanMax);
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving files", e);
        }
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

    public void initialise() throws StateStoreException {
        RevisionId firstRevisionId = new RevisionId(FIRST_REVISION, UUID.randomUUID().toString());
        String path = getFilesPath(firstRevisionId);
        try {
            LOGGER.debug("Writing initial empty file (revisionId = {}, path = {})", firstRevisionId, path);
            writeS3FileInfosToParquet(Collections.emptyList(), path);
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
    public void clearFileData() {
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
        Record record = new Record();
        record.put("fileName", s3FileInfo.getFilename());
        record.put("referencesJson", serDe.listToJson(s3FileInfo.getInternalReferences()));
        record.put("externalReferences", s3FileInfo.getExternalReferenceCount());
        record.put("lastStateStoreUpdateTime", s3FileInfo.getLastUpdateTime().toEpochMilli());
        return record;
    }

    private S3FileInfo getS3FileInfoFromRecord(Record record) {
        return S3FileInfo.builder()
                .filename((String) record.get("fileName"))
                .internalReferences(serDe.listFromJson((String) record.get("referencesJson")))
                .externalReferenceCount((int) record.get("externalReferences"))
                .lastUpdateTime(Instant.ofEpochMilli((long) record.get("lastStateStoreUpdateTime")))
                .build();
    }

    private void writeS3FileInfosToParquet(List<S3FileInfo> fileInfos, String path) throws IOException {
        LOGGER.debug("Writing {} file records to {}", fileInfos.size(), path);
        ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), FILE_SCHEMA, conf);

        for (S3FileInfo fileInfo : fileInfos) {
            recordWriter.write(getRecordFromS3FileInfo(fileInfo));
        }
        recordWriter.close();
        LOGGER.debug("Wrote {} file records to {}", fileInfos.size(), path);
    }

    private List<S3FileInfo> readS3FileInfosFromParquet(String path) throws IOException {
        LOGGER.debug("Loading file records from {}", path);
        List<S3FileInfo> fileInfos = new ArrayList<>();
        try (ParquetReader<Record> reader = fileInfosReader(path)) {
            ParquetReaderIterator recordReader = new ParquetReaderIterator(reader);
            while (recordReader.hasNext()) {
                fileInfos.add(getS3FileInfoFromRecord(recordReader.next()));
            }
        }
        LOGGER.debug("Loaded {} file records from {}", fileInfos.size(), path);
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
