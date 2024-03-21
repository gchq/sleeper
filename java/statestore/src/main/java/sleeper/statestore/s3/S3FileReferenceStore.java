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
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.SplitRequestsFailedException;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.statestore.AllReferencesToAFile.fileWithOneReference;
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
    private final S3RevisionIdStore s3RevisionIdStore;
    private final FileReferenceSerDe serDe = new FileReferenceSerDe();
    private final S3StateStoreDataFile<List<AllReferencesToAFile>> s3StateStoreFile;
    private Clock clock = Clock.systemUTC();

    private S3FileReferenceStore(Builder builder) {
        this.stateStorePath = Objects.requireNonNull(builder.stateStorePath, "stateStorePath must not be null");
        this.conf = Objects.requireNonNull(builder.conf, "hadoopConfiguration must not be null");
        this.s3RevisionIdStore = Objects.requireNonNull(builder.s3RevisionIdStore, "s3RevisionIdStore must not be null");
        s3StateStoreFile = S3StateStoreDataFile.builder()
                .revisionStore(s3RevisionIdStore)
                .description("files")
                .revisionIdKey(CURRENT_FILES_REVISION_ID_KEY)
                .buildPathFromRevisionId(this::getFilesPath)
                .loadAndWriteData(this::readFilesFromParquet, this::writeFilesToParquet)
                .hadoopConf(conf)
                .build();
    }

    static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        Instant updateTime = clock.instant();
        Set<String> newFiles = files.stream()
                .map(AllReferencesToAFile::getFilename)
                .collect(Collectors.toUnmodifiableSet());
        FileReferencesConditionCheck condition = list -> list.stream()
                .map(existingFile -> {
                    if (newFiles.contains(existingFile.getFilename())) {
                        return new FileAlreadyExistsException(existingFile.getFilename());
                    }
                    return null;
                }).filter(Objects::nonNull).findFirst();
        Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update = list -> Stream.concat(list.stream(), files.stream()
                .map(file -> file.withCreatedUpdateTime(updateTime)))
                .collect(toUnmodifiableList());
        updateS3Files(update, condition);
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException {
        try {
            updateS3Files(
                    buildSplitFileReferencesUpdate(splitRequests, clock.instant()),
                    buildSplitFileReferencesConditionCheck(splitRequests));
        } catch (StateStoreException e) {
            if (e instanceof SplitRequestsFailedException) {
                throw (SplitRequestsFailedException) e;
            } else {
                throw new SplitRequestsFailedException(List.of(), splitRequests, e);
            }
        }
    }

    private static FileReferencesConditionCheck buildSplitFileReferencesConditionCheck(List<SplitFileReferenceRequest> splitRequests) {
        return list -> {
            Map<String, FileReference> activePartitionFiles = new HashMap<>();
            for (AllReferencesToAFile existingFile : list) {
                for (FileReference reference : existingFile.getInternalReferences()) {
                    activePartitionFiles.put(getPartitionIdAndFilename(reference), reference);
                }
            }
            Set<String> activeFilenames = list.stream().map(AllReferencesToAFile::getFilename).collect(Collectors.toSet());
            int index = 0;
            for (SplitFileReferenceRequest splitRequest : splitRequests) {
                if (!activeFilenames.contains(splitRequest.getOldReference().getFilename())) {
                    return splitRequestsFailed(splitRequests, index, new FileNotFoundException(splitRequest.getOldReference().getFilename()));
                }
                String oldPartitionAndFilename = getPartitionIdAndFilename(splitRequest.getOldReference());
                if (!activePartitionFiles.containsKey(oldPartitionAndFilename)) {
                    return splitRequestsFailed(splitRequests, index, new FileReferenceNotFoundException(splitRequest.getOldReference()));
                }
                for (FileReference newFileReference : splitRequest.getNewReferences()) {
                    String newPartitionAndFilename = getPartitionIdAndFilename(newFileReference);
                    if (activePartitionFiles.containsKey(newPartitionAndFilename)) {
                        return splitRequestsFailed(splitRequests, index, new FileReferenceAlreadyExistsException(newFileReference));
                    }
                }
                FileReference existingOldReference = activePartitionFiles.get(oldPartitionAndFilename);
                if (existingOldReference.getJobId() != null) {
                    return splitRequestsFailed(splitRequests, index, new FileReferenceAssignedToJobException(existingOldReference));
                }
                index++;
            }
            return Optional.empty();
        };
    }

    private static Optional<SplitRequestsFailedException> splitRequestsFailed(List<SplitFileReferenceRequest> splitRequests, int requestIndex, StateStoreException cause) {
        return Optional.of(new SplitRequestsFailedException(splitRequests.subList(0, requestIndex), splitRequests.subList(requestIndex, splitRequests.size()), cause));
    }

    private static Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> buildSplitFileReferencesUpdate(List<SplitFileReferenceRequest> splitRequests, Instant updateTime) {
        Map<String, List<SplitFileReferenceRequest>> requestsByFilename = splitRequests.stream()
                .collect(groupingBy(request -> request.getOldReference().getFilename()));
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
    public void atomicallyReplaceFileReferencesWithNewOne(
            String jobId, String partitionId, List<String> inputFiles, FileReference newReference) throws StateStoreException {
        Instant updateTime = clock.instant();
        Set<String> inputFilesSet = new HashSet<>(inputFiles);
        FileReference.validateNewReferenceForJobOutput(inputFilesSet, newReference);
        FileReferencesConditionCheck condition = list -> {
            Map<String, AllReferencesToAFile> filesByName = list.stream()
                    .collect(Collectors.toMap(AllReferencesToAFile::getFilename, Function.identity()));
            Map<String, FileReference> activePartitionFiles = new HashMap<>();
            for (AllReferencesToAFile existingFile : list) {
                for (FileReference reference : existingFile.getInternalReferences()) {
                    activePartitionFiles.put(getPartitionIdAndFilename(reference), reference);
                }
            }
            StateStoreException exception = null;
            if (filesByName.containsKey(newReference.getFilename())) {
                exception = new FileAlreadyExistsException(newReference.getFilename());
            }
            for (String filename : inputFiles) {
                if (!filesByName.containsKey(filename)) {
                    exception = new FileNotFoundException(filename);
                } else if (!activePartitionFiles.containsKey(partitionId + DELIMITER + filename)) {
                    exception = new FileReferenceNotFoundException(filename, partitionId);
                } else {
                    FileReference fileReference = activePartitionFiles.get(partitionId + DELIMITER + filename);
                    if (!jobId.equals(fileReference.getJobId())) {
                        exception = new FileReferenceNotAssignedToJobException(fileReference, jobId);
                    }
                }
            }
            return Optional.ofNullable(exception);
        };

        Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update = existingFiles -> Stream.concat(
                existingFiles.stream().map(existingFile -> {
                    if (inputFilesSet.contains(existingFile.getFilename())) {
                        return existingFile.removeReferenceForPartition(partitionId, updateTime);
                    } else {
                        return existingFile;
                    }
                }),
                Stream.of(fileWithOneReference(newReference, updateTime)))
                .collect(Collectors.toUnmodifiableList());
        updateS3Files(update, condition);
    }

    @Override
    public void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException {
        Instant updateTime = clock.instant();
        Map<String, List<AssignJobIdRequest>> requestsByFilename = requests.stream()
                .flatMap(request -> request.getFilenames().stream()
                        .map(filename -> entry(filename, request)))
                .collect(groupingBy(Map.Entry::getKey,
                        mapping(Map.Entry::getValue, toUnmodifiableList())));

        FileReferencesConditionCheck condition = list -> {
            Map<String, AllReferencesToAFile> existingFileByName = list.stream()
                    .collect(Collectors.toMap(AllReferencesToAFile::getFilename, identity()));
            for (AssignJobIdRequest request : requests) {
                for (String filename : request.getFilenames()) {
                    AllReferencesToAFile existingFile = existingFileByName.get(filename);
                    if (existingFile == null) {
                        return Optional.of(new FileReferenceNotFoundException(filename, request.getPartitionId()));
                    }
                    FileReference existingReference = existingFile.getReferenceForPartitionId(request.getPartitionId()).orElse(null);
                    if (existingReference == null) {
                        return Optional.of(new FileReferenceNotFoundException(filename, request.getPartitionId()));
                    }
                    if (existingReference.getJobId() != null) {
                        return Optional.of(new FileReferenceAssignedToJobException(existingReference));
                    }
                }
            }
            return Optional.empty();
        };
        Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update = list -> {
            List<AllReferencesToAFile> filteredFiles = new ArrayList<>();
            for (AllReferencesToAFile existing : list) {
                List<AssignJobIdRequest> fileRequests = requestsByFilename.get(existing.getFilename());
                if (fileRequests == null) {
                    filteredFiles.add(existing);
                } else {
                    AllReferencesToAFile file = existing;
                    for (AssignJobIdRequest fileRequest : fileRequests) {
                        file = file.withJobIdForPartition(fileRequest.getJobId(), fileRequest.getPartitionId(), updateTime);
                    }
                    filteredFiles.add(file);
                }
            }
            return filteredFiles;
        };

        updateS3Files(update, condition);
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        Set<String> filenamesSet = new HashSet<>(filenames);
        FileReferencesConditionCheck condition = list -> {
            StateStoreException exception = null;
            List<AllReferencesToAFile> references = list.stream()
                    .filter(file -> filenamesSet.contains(file.getFilename()))
                    .collect(Collectors.toUnmodifiableList());
            Set<String> missingFilenames = new HashSet<>(filenames);
            references.stream().map(AllReferencesToAFile::getFilename).forEach(missingFilenames::remove);
            if (!missingFilenames.isEmpty()) {
                exception = new FileNotFoundException(missingFilenames.stream().findFirst().orElseThrow());
            }
            for (AllReferencesToAFile reference : references) {
                if (reference.getTotalReferenceCount() > 0) {
                    exception = new FileHasReferencesException(reference);
                }
            }
            return Optional.ofNullable(exception);
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
    public AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
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

    private void updateS3Files(Function<List<AllReferencesToAFile>, List<AllReferencesToAFile>> update,
            FileReferencesConditionCheck condition) throws StateStoreException {
        s3StateStoreFile.updateWithAttempts(10, update, condition);
    }

    interface FileReferencesConditionCheck extends S3StateStoreDataFile.ConditionCheck<List<AllReferencesToAFile>> {
    }

    private S3RevisionId getCurrentFilesRevisionId() {
        return s3RevisionIdStore.getCurrentFilesRevisionId();
    }

    public void initialise() throws StateStoreException {
        S3RevisionId firstRevisionId = S3RevisionId.firstRevision(UUID.randomUUID().toString());
        String path = getFilesPath(firstRevisionId);
        LOGGER.debug("Writing initial empty file (revisionId = {}, path = {})", firstRevisionId, path);
        writeFilesToParquet(Collections.emptyList(), path);
        s3RevisionIdStore.saveFirstFilesRevision(firstRevisionId);
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
        s3RevisionIdStore.deleteFilesRevision();
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
        private S3RevisionIdStore s3RevisionIdStore;

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

        Builder s3RevisionIdStore(S3RevisionIdStore s3RevisionIdStore) {
            this.s3RevisionIdStore = s3RevisionIdStore;
            return this;
        }

        S3FileReferenceStore build() {
            return new S3FileReferenceStore(this);
        }
    }
}
