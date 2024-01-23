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
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.ReferencedFile;
import sleeper.core.statestore.SplitFileReferenceRequest;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.statestore.s3.S3RevisionUtils.RevisionId;
import static sleeper.statestore.s3.S3StateStore.FIRST_REVISION;

class S3FileReferenceStore implements FileReferenceStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileReferenceStore.class);
    private static final Schema FILE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("fileName", new StringType()))
            .valueFields(
                    new Field("referencesJson", new StringType()),
                    new Field("externalReferences", new IntType()),
                    new Field("lastStateStoreUpdateTime", new LongType()))
            .build();
    private static final String DELIMITER = "|";

    private final String stateStorePath;
    private final Configuration conf;
    private final S3RevisionUtils s3RevisionUtils;
    private final FileReferenceSerDe serDe = new FileReferenceSerDe();
    private Clock clock = Clock.systemUTC();

    private S3FileReferenceStore(Builder builder) {
        this.stateStorePath = Objects.requireNonNull(builder.stateStorePath, "stateStorePath must not be null");
        this.conf = Objects.requireNonNull(builder.conf, "hadoopConfiguration must not be null");
        this.s3RevisionUtils = Objects.requireNonNull(builder.s3RevisionUtils, "s3RevisionUtils must not be null");
    }

    static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFile(FileReference fileReference) throws StateStoreException {
        addFiles(Collections.singletonList(fileReference));
    }

    @Override
    public void addFiles(List<FileReference> fileReferences) throws StateStoreException {
        Instant updateTime = clock.instant();
        Map<String, FileReference> newFilesByPartitionAndFilename = fileReferences.stream()
                .collect(Collectors.toMap(
                        S3FileReferenceStore::getPartitionIdAndFilename,
                        Function.identity()));
        Function<List<ReferencedFile>, String> condition = list -> list.stream()
                .flatMap(file -> file.getInternalReferences().stream())
                .map(existingFile -> {
                    String partitionIdAndName = getPartitionIdAndFilename(existingFile);
                    if (newFilesByPartitionAndFilename.containsKey(partitionIdAndName)) {
                        return "File already in system: " + newFilesByPartitionAndFilename.get(partitionIdAndName);
                    }
                    return null;
                }).filter(Objects::nonNull)
                .findFirst().orElse("");
        Function<List<ReferencedFile>, List<ReferencedFile>> update = list -> {
            list.addAll(ReferencedFile.listNewFilesWithReferences(fileReferences, updateTime));
            return list;
        };
        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file references", e);
        }
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException {
        try {
            updateS3Files(
                    buildSplitFileReferencesUpdate(splitRequests, clock.instant()),
                    buildSplitFileReferencesCondition(splitRequests));
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file references", e);
        }
    }

    private static Function<List<ReferencedFile>, String> buildSplitFileReferencesCondition(List<SplitFileReferenceRequest> splitRequests) {
        Map<String, List<SplitFileReferenceRequest>> splitRequestByPartitionIdAndFilename = splitRequests.stream()
                .collect(Collectors.groupingBy(
                        splitRequest -> getPartitionIdAndFilename(splitRequest.getOldReference())));
        return list -> {
            Map<String, FileReference> activePartitionFiles = new HashMap<>();
            for (ReferencedFile existingFile : list) {
                for (FileReference reference : existingFile.getInternalReferences()) {
                    activePartitionFiles.put(getPartitionIdAndFilename(reference), reference);
                }
            }
            return splitRequestByPartitionIdAndFilename.values().stream()
                    .flatMap(List::stream)
                    .map(splitFileRequest -> {
                        String oldPartitionAndFilename = getPartitionIdAndFilename(splitFileRequest.getOldReference());
                        if (!activePartitionFiles.containsKey(oldPartitionAndFilename)) {
                            return "File to split was not found with partitionId and filename: " + oldPartitionAndFilename;
                        }
                        for (FileReference newFileReference : splitFileRequest.getNewReferences()) {
                            String newPartitionAndFilename = getPartitionIdAndFilename(newFileReference);
                            if (activePartitionFiles.containsKey(newPartitionAndFilename)) {
                                return "File reference already exists with partitionId and filename: " + newPartitionAndFilename;
                            }
                        }
                        return "";
                    }).findFirst().orElse("");
        };
    }

    private static Function<List<ReferencedFile>, List<ReferencedFile>> buildSplitFileReferencesUpdate(List<SplitFileReferenceRequest> splitRequests, Instant updateTime) {
        Map<String, List<SplitFileReferenceRequest>> requestsByFilename = splitRequests.stream()
                .collect(Collectors.groupingBy(request -> request.getOldReference().getFilename()));
        return list -> list.stream()
                .map(file -> {
                    List<SplitFileReferenceRequest> requests = requestsByFilename.get(file.getFilename());
                    if (requests == null) {
                        return file;
                    }
                    for (SplitFileReferenceRequest request : requests) {
                        file = file.splitReferenceFromPartition(
                                request.getOldReference().getPartitionId(), request.getNewReferences(), updateTime);
                    }
                    return file;
                }).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
            String jobId, String partitionId, List<String> filesToBeMarkedReadyForGC, List<FileReference> newFiles) throws StateStoreException {
        Instant updateTime = clock.instant();
        Set<String> filesToBeMarkedReadyForGCSet = new HashSet<>(filesToBeMarkedReadyForGC);

        Function<List<ReferencedFile>, String> condition = list -> {
            Map<String, FileReference> activePartitionFiles = new HashMap<>();
            for (ReferencedFile existingFile : list) {
                for (FileReference reference : existingFile.getInternalReferences()) {
                    activePartitionFiles.put(getPartitionIdAndFilename(reference), reference);
                }
            }
            for (String filename : filesToBeMarkedReadyForGC) {
                if (!activePartitionFiles.containsKey(partitionId + DELIMITER + filename)) {
                    return "Files in filesToBeMarkedReadyForGC should be active: file " + filename + " is not active in partition " + partitionId;
                } else if (!jobId.equals(activePartitionFiles.get(partitionId + DELIMITER + filename).getJobId())) {
                    return "Files in filesToBeMarkedReadyForGC should be assigned jobId " + jobId;
                }
            }
            return "";
        };

        Function<List<ReferencedFile>, List<ReferencedFile>> update = list -> {
            List<ReferencedFile> newReferencedFiles = ReferencedFile.listNewFilesWithReferences(newFiles, updateTime);
            Map<String, ReferencedFile> newFilesByName = newReferencedFiles.stream()
                    .collect(Collectors.toMap(ReferencedFile::getFilename, Function.identity()));
            List<ReferencedFile> after = new ArrayList<>();
            Set<String> filenamesWithUpdatedReferences = new HashSet<>();
            for (ReferencedFile existingFile : list) {
                ReferencedFile file = existingFile;
                if (filesToBeMarkedReadyForGCSet.contains(existingFile.getFilename())) {
                    file = file.removeReferenceForPartition(partitionId, updateTime);
                }
                ReferencedFile newFile = newFilesByName.get(existingFile.getFilename());
                if (newFile != null) {
                    file = file.addReferences(newFile.getInternalReferences(), updateTime);
                    filenamesWithUpdatedReferences.add(existingFile.getFilename());
                }
                after.add(file);
            }
            return Stream.concat(
                            after.stream(),
                            newReferencedFiles.stream().filter(file -> !filenamesWithUpdatedReferences.contains(file.getFilename())))
                    .collect(Collectors.toUnmodifiableList());
        };
        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file references", e);
        }
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileReference> fileReferences) throws StateStoreException {
        Instant updateTime = clock.instant();
        Set<String> partitionAndNames = fileReferences.stream()
                .map(S3FileReferenceStore::getPartitionIdAndFilename)
                .collect(Collectors.toSet());
        Map<String, Set<String>> partitionUpdatesByName = fileReferences.stream()
                .collect(Collectors.groupingBy(FileReference::getFilename,
                        Collectors.mapping(FileReference::getPartitionId, Collectors.toUnmodifiableSet())));

        Function<List<ReferencedFile>, String> condition = list -> {
            Set<String> missing = new HashSet<>(partitionAndNames);
            for (ReferencedFile existing : list) {
                for (FileReference reference : existing.getInternalReferences()) {
                    String partitionAndName = getPartitionIdAndFilename(reference);
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

        Function<List<ReferencedFile>, List<ReferencedFile>> update = list -> {
            List<ReferencedFile> filteredFiles = new ArrayList<>();
            for (ReferencedFile existing : list) {
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
            throw new StateStoreException("IOException updating file references", e);
        } catch (StateStoreException e) {
            throw new StateStoreException("StateStoreException updating jobid of files", e);
        }
    }


    @Override
    public void deleteReadyForGCFiles(List<String> filenames) throws StateStoreException {
        Set<String> filenamesSet = new HashSet<>(filenames);
        Function<List<ReferencedFile>, String> condition = list -> {
            List<ReferencedFile> references = list.stream()
                    .filter(file -> filenamesSet.contains(file.getFilename()))
                    .collect(Collectors.toUnmodifiableList());
            Set<String> missingFilenames = new HashSet<>(filenames);
            references.stream().map(ReferencedFile::getFilename).forEach(missingFilenames::remove);
            if (!missingFilenames.isEmpty()) {
                return "Could not find files: " + missingFilenames;
            }
            return references.stream()
                    .filter(f -> f.getTotalReferenceCount() > 0)
                    .findAny().map(f -> "File to be deleted should be marked as ready for GC, found active file " + f.getFilename())
                    .orElse("");
        };

        Function<List<ReferencedFile>, List<ReferencedFile>> update = list -> list.stream()
                .filter(file -> !filenamesSet.contains(file.getFilename()))
                .collect(Collectors.toUnmodifiableList());

        try {
            updateS3Files(update, condition);
        } catch (IOException e) {
            throw new StateStoreException("IOException updating file references", e);
        }
    }

    @Override
    public List<FileReference> getActiveFiles() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        RevisionId revisionId = getCurrentFilesRevisionId();
        if (null == revisionId) {
            return Collections.emptyList();
        }
        try {
            List<ReferencedFile> files = readFilesFromParquet(getFilesPath(revisionId));
            return files.stream()
                    .flatMap(file -> file.getInternalReferences().stream())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files", e);
        }
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        try {
            List<ReferencedFile> files = readFilesFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            return files.stream()
                    .filter(file -> file.getTotalReferenceCount() == 0 && file.getLastUpdateTime().isBefore(maxUpdateTime))
                    .map(ReferencedFile::getFilename).distinct();
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving ready for GC files", e);
        }
    }

    @Override
    public List<FileReference> getActiveFilesWithNoJobId() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        try {
            List<ReferencedFile> files = readFilesFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            return files.stream()
                    .flatMap(file -> file.getInternalReferences().stream())
                    .filter(f -> f.getJobId() == null)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new StateStoreException("IOException retrieving active files with no job id", e);
        }
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        List<FileReference> files = getActiveFiles();
        Map<String, List<String>> partitionToFiles = new HashMap<>();
        for (FileReference fileReference : files) {
            String partition = fileReference.getPartitionId();
            if (!partitionToFiles.containsKey(partition)) {
                partitionToFiles.put(partition, new ArrayList<>());
            }
            partitionToFiles.get(partition).add(fileReference.getFilename());
        }
        return partitionToFiles;
    }

    @Override
    public AllFileReferences getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        try {
            List<ReferencedFile> files = readFilesFromParquet(getFilesPath(getCurrentFilesRevisionId()));
            Set<FileReference> activeFiles = files.stream()
                    .flatMap(file -> file.getInternalReferences().stream())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            List<String> filesWithNoReferences = files.stream()
                    .filter(file -> file.getTotalReferenceCount() == 0)
                    .map(ReferencedFile::getFilename)
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

    private void updateS3Files(Function<List<ReferencedFile>, List<ReferencedFile>> update, Function<List<ReferencedFile>, String> condition)
            throws IOException, StateStoreException {
        int numberAttempts = 0;
        while (numberAttempts < 10) {
            RevisionId revisionId = getCurrentFilesRevisionId();
            String filesPath = getFilesPath(revisionId);
            List<ReferencedFile> files;
            try {
                files = readFilesFromParquet(filesPath);
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
            List<ReferencedFile> updatedFiles = update.apply(files);
            LOGGER.debug("Applied update to file information");

            // Attempt to write update
            RevisionId nextRevisionId = s3RevisionUtils.getNextRevisionId(revisionId);
            String nextRevisionIdPath = getFilesPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated file information (revisionId = {}, path = {})",
                        nextRevisionId, nextRevisionIdPath);
                writeFilesToParquet(updatedFiles, nextRevisionIdPath);
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
            writeFilesToParquet(Collections.emptyList(), path);
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
        try (ParquetReader<Record> reader = fileReader(path)) {
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

    private Record getRecordFromFile(ReferencedFile file) {
        Record record = new Record();
        record.put("fileName", file.getFilename());
        record.put("referencesJson", serDe.setToJson(file.getInternalReferences()));
        record.put("externalReferences", file.getExternalReferenceCount());
        record.put("lastStateStoreUpdateTime", file.getLastUpdateTime().toEpochMilli());
        return record;
    }

    private ReferencedFile getFileFromRecord(Record record) {
        Set<FileReference> internalReferences = serDe.setFromJson((String) record.get("referencesJson"));
        return ReferencedFile.builder()
                .filename((String) record.get("fileName"))
                .internalReferences(internalReferences)
                .totalReferenceCount((int) record.get("externalReferences") + internalReferences.size())
                .lastUpdateTime(Instant.ofEpochMilli((long) record.get("lastStateStoreUpdateTime")))
                .build();
    }

    private void writeFilesToParquet(List<ReferencedFile> files, String path) throws IOException {
        LOGGER.debug("Writing {} file records to {}", files.size(), path);
        ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), FILE_SCHEMA, conf);

        for (ReferencedFile file : files) {
            recordWriter.write(getRecordFromFile(file));
        }
        recordWriter.close();
        LOGGER.debug("Wrote {} file records to {}", files.size(), path);
    }

    private List<ReferencedFile> readFilesFromParquet(String path) throws IOException {
        LOGGER.debug("Loading file records from {}", path);
        List<ReferencedFile> files = new ArrayList<>();
        try (ParquetReader<Record> reader = fileReader(path)) {
            ParquetReaderIterator recordReader = new ParquetReaderIterator(reader);
            while (recordReader.hasNext()) {
                files.add(getFileFromRecord(recordReader.next()));
            }
        }
        LOGGER.debug("Loaded {} file records from {}", files.size(), path);
        return files;
    }

    private ParquetReader<Record> fileReader(String path) throws IOException {
        return new ParquetRecordReader.Builder(new Path(path), FILE_SCHEMA)
                .withConf(conf)
                .build();
    }

    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private static String getPartitionIdAndFilename(FileReference fileReference) {
        return fileReference.getPartitionId() + DELIMITER + fileReference.getFilename();
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

        S3FileReferenceStore build() {
            return new S3FileReferenceStore(this);
        }
    }
}
