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
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.statestore.FileReferenceStore;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.statestore.s3.S3StateStore.CURRENT_FILES_REVISION_ID_KEY;

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
    private final S3RevisionStore s3RevisionStore;
    private final FileReferenceSerDe serDe = new FileReferenceSerDe();
    private final RevisionTrackedS3FileType<List<AllReferencesToAFile>> s3FileType;
    private Clock clock = Clock.systemUTC();

    private S3FileReferenceStore(Builder builder) {
        this.stateStorePath = Objects.requireNonNull(builder.stateStorePath, "stateStorePath must not be null");
        this.conf = Objects.requireNonNull(builder.conf, "hadoopConfiguration must not be null");
        this.s3RevisionStore = Objects.requireNonNull(builder.s3RevisionStore, "s3RevisionUtils must not be null");
        s3FileType = RevisionTrackedS3FileType.builder()
                .description("files")
                .revisionIdKey(CURRENT_FILES_REVISION_ID_KEY)
                .buildPathFromRevisionId(this::getFilesPath)
                .store(new RevisionTrackedS3FileStore<>(
                        this::readFilesFromParquet, this::writeFilesToParquet, conf))
                .build();
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
        Function<List<AllReferencesToAFile>, String> condition = list -> list.stream()
                .flatMap(file -> file.getInternalReferences().stream())
                .map(existingFile -> {
                    String partitionIdAndName = getPartitionIdAndFilename(existingFile);
                    if (newFilesByPartitionAndFilename.containsKey(partitionIdAndName)) {
                        return "File already in system: " + newFilesByPartitionAndFilename.get(partitionIdAndName);
                    }
                    return null;
                }).filter(Objects::nonNull)
                .findFirst().orElse("");

        Map<String, List<FileReference>> newReferencesByFilename = fileReferences.stream()
                .collect(Collectors.groupingBy(FileReference::getFilename));
        Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update = list -> {
            List<AllReferencesToAFile> updatedFiles = new ArrayList<>(list.size() + newReferencesByFilename.size());
            for (AllReferencesToAFile file : list) {
                List<FileReference> newReferences = newReferencesByFilename.get(file.getFilename());
                if (newReferences != null) {
                    file = file.addReferences(newReferences, updateTime);
                    newReferencesByFilename.remove(file.getFilename());
                }
                updatedFiles.add(file);
            }
            AllReferencesToAFile.newFilesWithReferences(
                    newReferencesByFilename.values().stream().flatMap(List::stream),
                    updateTime
            ).forEach(updatedFiles::add);
            return updatedFiles;
        };
        updateS3Files(update, condition);
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException {
        updateS3Files(
                buildSplitFileReferencesUpdate(splitRequests, clock.instant()),
                buildSplitFileReferencesCondition(splitRequests));
    }

    private static Function<List<AllReferencesToAFile>, String> buildSplitFileReferencesCondition(List<SplitFileReferenceRequest> splitRequests) {
        Map<String, List<SplitFileReferenceRequest>> splitRequestByPartitionIdAndFilename = splitRequests.stream()
                .collect(Collectors.groupingBy(
                        splitRequest -> getPartitionIdAndFilename(splitRequest.getOldReference())));
        return list -> {
            Map<String, FileReference> activePartitionFiles = new HashMap<>();
            for (AllReferencesToAFile existingFile : list) {
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
                        FileReference existingOldReference = activePartitionFiles.get(oldPartitionAndFilename);
                        if (existingOldReference.getJobId() != null) {
                            return "File is already assigned to a compaction job with id: " + existingOldReference.getJobId();
                        }
                        return "";
                    }).findFirst().orElse("");
        };
    }

    private static Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> buildSplitFileReferencesUpdate(List<SplitFileReferenceRequest> splitRequests, Instant updateTime) {
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
    public void atomicallyApplyJobFileReferenceUpdates(
            String jobId, String partitionId, List<String> filesProcessed, List<FileReference> newReferences) throws StateStoreException {
        Instant updateTime = clock.instant();
        Set<String> inputFilesSet = new HashSet<>(filesProcessed);
        FileReference.validateNewReferencesForJobOutput(inputFilesSet, newReferences);

        Function<List<AllReferencesToAFile>, String> condition = list -> {
            Map<String, FileReference> activePartitionFiles = new HashMap<>();
            for (AllReferencesToAFile existingFile : list) {
                for (FileReference reference : existingFile.getInternalReferences()) {
                    activePartitionFiles.put(getPartitionIdAndFilename(reference), reference);
                }
            }
            for (String filename : filesProcessed) {
                if (!activePartitionFiles.containsKey(partitionId + DELIMITER + filename)) {
                    return "Files in filesToBeMarkedReadyForGC should be active: file " + filename + " is not active in partition " + partitionId;
                } else if (!jobId.equals(activePartitionFiles.get(partitionId + DELIMITER + filename).getJobId())) {
                    return "Files in filesToBeMarkedReadyForGC should be assigned jobId " + jobId;
                }
            }
            return "";
        };

        List<AllReferencesToAFile> newFiles = AllReferencesToAFile.listNewFilesWithReferences(newReferences, updateTime);
        Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update = list -> {
            List<AllReferencesToAFile> after = new ArrayList<>();
            for (AllReferencesToAFile existingFile : list) {
                AllReferencesToAFile file = existingFile;
                if (inputFilesSet.contains(existingFile.getFilename())) {
                    file = file.removeReferenceForPartition(partitionId, updateTime);
                }
                after.add(file);
            }
            return Stream.concat(
                            after.stream(),
                            newFiles.stream())
                    .collect(Collectors.toUnmodifiableList());
        };
        updateS3Files(update, condition);
    }

    @Override
    public void atomicallyAssignJobIdToFileReferences(String jobId, List<FileReference> fileReferences) throws StateStoreException {
        Instant updateTime = clock.instant();
        Set<String> partitionAndNames = fileReferences.stream()
                .map(S3FileReferenceStore::getPartitionIdAndFilename)
                .collect(Collectors.toSet());
        Map<String, Set<String>> partitionUpdatesByName = fileReferences.stream()
                .collect(Collectors.groupingBy(FileReference::getFilename,
                        Collectors.mapping(FileReference::getPartitionId, Collectors.toUnmodifiableSet())));

        Function<List<AllReferencesToAFile>, String> condition = list -> {
            Set<String> missing = new HashSet<>(partitionAndNames);
            for (AllReferencesToAFile existing : list) {
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

        Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update = list -> {
            List<AllReferencesToAFile> filteredFiles = new ArrayList<>();
            for (AllReferencesToAFile existing : list) {
                Set<String> partitionUpdates = partitionUpdatesByName.get(existing.getFilename());
                if (partitionUpdates == null) {
                    filteredFiles.add(existing);
                } else {
                    filteredFiles.add(existing.withJobIdForPartitions(jobId, partitionUpdates, updateTime));
                }
            }
            return filteredFiles;
        };

        updateS3Files(update, condition);
    }


    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        Set<String> filenamesSet = new HashSet<>(filenames);
        Function<List<AllReferencesToAFile>, String> condition = list -> {
            List<AllReferencesToAFile> references = list.stream()
                    .filter(file -> filenamesSet.contains(file.getFilename()))
                    .collect(Collectors.toUnmodifiableList());
            Set<String> missingFilenames = new HashSet<>(filenames);
            references.stream().map(AllReferencesToAFile::getFilename).forEach(missingFilenames::remove);
            if (!missingFilenames.isEmpty()) {
                return "Could not find files: " + missingFilenames;
            }
            return references.stream()
                    .filter(f -> f.getTotalReferenceCount() > 0)
                    .findAny().map(f -> "File to be deleted should be marked as ready for GC, found active file " + f.getFilename())
                    .orElse("");
        };

        Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update = list -> list.stream()
                .filter(file -> !filenamesSet.contains(file.getFilename()))
                .collect(Collectors.toUnmodifiableList());

        updateS3Files(update, condition);
    }

    @Override
    public List<FileReference> getFileReferences() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        S3RevisionId revisionId = getCurrentFilesRevisionId();
        if (null == revisionId) {
            return Collections.emptyList();
        }
        List<AllReferencesToAFile> files = readFilesFromParquet(getFilesPath(revisionId));
        return files.stream()
                .flatMap(file -> file.getInternalReferences().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        List<AllReferencesToAFile> files = readFilesFromParquet(getFilesPath(getCurrentFilesRevisionId()));
        return files.stream()
                .filter(file -> file.getTotalReferenceCount() == 0 && file.getLastStateStoreUpdateTime().isBefore(maxUpdateTime))
                .map(AllReferencesToAFile::getFilename).distinct();
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException {
        // TODO Optimise the following by pushing the predicate down to the Parquet reader
        List<AllReferencesToAFile> files = readFilesFromParquet(getFilesPath(getCurrentFilesRevisionId()));
        return files.stream()
                .flatMap(file -> file.getInternalReferences().stream())
                .filter(f -> f.getJobId() == null)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, List<String>> getPartitionToReferencedFilesMap() throws StateStoreException {
        List<FileReference> files = getFileReferences();
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
    public AllReferencesToAllFiles getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        List<AllReferencesToAFile> allFiles = readFilesFromParquet(getFilesPath(getCurrentFilesRevisionId()));
        List<AllReferencesToAFile> filesWithNoReferences = allFiles.stream()
                .filter(file -> file.getTotalReferenceCount() < 1)
                .collect(toUnmodifiableList());
        List<AllReferencesToAFile> resultFiles = Stream.concat(
                        allFiles.stream()
                                .filter(file -> file.getTotalReferenceCount() > 0),
                        filesWithNoReferences.stream().limit(maxUnreferencedFiles))
                .collect(toUnmodifiableList());
        return new AllReferencesToAllFiles(resultFiles, filesWithNoReferences.size() > maxUnreferencedFiles);
    }

    private void updateS3Files(Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update, Function<List<AllReferencesToAFile>, String> condition)
            throws StateStoreException {
        UpdateS3File.updateWithAttempts(s3RevisionStore, s3FileType, 10, update, condition);
    }

    private S3RevisionId getCurrentFilesRevisionId() {
        return s3RevisionStore.getCurrentFilesRevisionId();
    }

    public void initialise() throws StateStoreException {
        S3RevisionId firstRevisionId = S3RevisionId.firstRevision(UUID.randomUUID().toString());
        String path = getFilesPath(firstRevisionId);
        LOGGER.debug("Writing initial empty file (revisionId = {}, path = {})", firstRevisionId, path);
        writeFilesToParquet(Collections.emptyList(), path);
        s3RevisionStore.saveFirstFilesRevision(firstRevisionId);
    }

    @Override
    public boolean hasNoFiles() {
        S3RevisionId revisionId = getCurrentFilesRevisionId();
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
        s3RevisionStore.deleteFilesRevision();
    }

    private String getFilesPath(S3RevisionId revisionId) {
        return stateStorePath + "/files/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-files.parquet";
    }

    private Record getRecordFromFile(AllReferencesToAFile file) {
        Record record = new Record();
        record.put("fileName", file.getFilename());
        record.put("referencesJson", serDe.collectionToJson(file.getInternalReferences()));
        record.put("externalReferences", file.getExternalReferenceCount());
        record.put("lastStateStoreUpdateTime", file.getLastStateStoreUpdateTime().toEpochMilli());
        return record;
    }

    private AllReferencesToAFile getFileFromRecord(Record record) {
        List<FileReference> internalReferences = serDe.listFromJson((String) record.get("referencesJson"));
        return AllReferencesToAFile.builder()
                .filename((String) record.get("fileName"))
                .internalReferences(internalReferences)
                .totalReferenceCount((int) record.get("externalReferences") + internalReferences.size())
                .lastStateStoreUpdateTime(Instant.ofEpochMilli((long) record.get("lastStateStoreUpdateTime")))
                .build();
    }

    private void writeFilesToParquet(List<AllReferencesToAFile> files, String path) throws StateStoreException {
        LOGGER.debug("Writing {} file records to {}", files.size(), path);
        try (ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory
                .createParquetRecordWriter(new Path(path), FILE_SCHEMA, conf)) {
            for (AllReferencesToAFile file : files) {
                recordWriter.write(getRecordFromFile(file));
            }
        } catch (IOException e) {
            throw new StateStoreException("Failed writing files", e);
        }
        LOGGER.debug("Wrote {} file records to {}", files.size(), path);
    }

    private List<AllReferencesToAFile> readFilesFromParquet(String path) throws StateStoreException {
        LOGGER.debug("Loading file records from {}", path);
        List<AllReferencesToAFile> files = new ArrayList<>();
        try (ParquetReader<Record> reader = fileReader(path)) {
            ParquetReaderIterator recordReader = new ParquetReaderIterator(reader);
            while (recordReader.hasNext()) {
                files.add(getFileFromRecord(recordReader.next()));
            }
        } catch (IOException e) {
            throw new StateStoreException("Failed loading files", e);
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
        private S3RevisionStore s3RevisionStore;

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

        Builder s3RevisionUtils(S3RevisionStore s3RevisionStore) {
            this.s3RevisionStore = s3RevisionStore;
            return this;
        }

        S3FileReferenceStore build() {
            return new S3FileReferenceStore(this);
        }
    }
}
