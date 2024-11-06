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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.StateStoreFile;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.AssignJobIdsTransaction;
import sleeper.core.statestore.transactionlog.transactions.DeleteFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;
import sleeper.core.statestore.transactionlog.transactions.SplitFileReferencesTransaction;
import sleeper.statestore.StateStoreArrowFileStore;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.statestore.s3.S3StateStore.CURRENT_FILES_REVISION_ID_KEY;

/**
 * A Sleeper table file reference store where the state is held in S3, and revisions of the state are indexed in
 * DynamoDB.
 */
class S3FileReferenceStore implements FileReferenceStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileReferenceStore.class);
    private static final String DELIMITER = "|";

    private final String stateStorePath;
    private final Configuration conf;
    private final S3RevisionIdStore s3RevisionIdStore;
    private final S3StateStoreDataFile<StateStoreFiles> s3StateStoreFile;
    private final StateStoreArrowFileStore dataStore;
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
                .loadAndWriteData(this::readFiles, this::writeFiles)
                .hadoopConf(conf)
                .build();
        dataStore = new StateStoreArrowFileStore(conf);
    }

    static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        updateS3Files(clock.instant(), new AddFilesTransaction(files));
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException {
        try {
            updateS3Files(clock.instant(), new SplitFileReferencesTransaction(splitRequests));
        } catch (StateStoreException e) {
            throw new SplitRequestsFailedException(List.of(), splitRequests, e);
        }
    }

    @Override
    public void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException {
        try {
            updateS3Files(clock.instant(), new ReplaceFileReferencesTransaction(requests));
        } catch (StateStoreException e) {
            throw new ReplaceRequestsFailedException(requests, e);
        }
    }

    @Override
    public void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException {
        updateS3Files(clock.instant(), new AssignJobIdsTransaction(requests));
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        updateS3Files(clock.instant(), new DeleteFilesTransaction(filenames));
    }

    @Override
    public List<FileReference> getFileReferences() throws StateStoreException {
        S3RevisionId revisionId = getCurrentFilesRevisionId();
        if (null == revisionId) {
            return Collections.emptyList();
        }
        StateStoreFiles files = readFiles(getFilesPath(revisionId));
        return files.references()
                .collect(Collectors.toList());
    }

    @Override
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) throws StateStoreException {
        StateStoreFiles files = readFiles(getFilesPath(getCurrentFilesRevisionId()));
        return files.referencedAndUnreferenced().stream()
                .filter(file -> file.getReferences().isEmpty() && file.getLastStateStoreUpdateTime().isBefore(maxUpdateTime))
                .map(StateStoreFile::getFilename);
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException {
        StateStoreFiles files = readFiles(getFilesPath(getCurrentFilesRevisionId()));
        return files.references()
                .filter(f -> f.getJobId() == null)
                .collect(Collectors.toList());
    }

    @Override
    public AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        StateStoreFiles allFiles = readFiles(getFilesPath(getCurrentFilesRevisionId()));
        List<AllReferencesToAFile> filesWithNoReferences = allFiles.referencedAndUnreferenced().stream()
                .filter(file -> file.getReferences().isEmpty())
                .map(StateStoreFile::toModel)
                .collect(toUnmodifiableList());
        List<AllReferencesToAFile> resultFiles = Stream.concat(
                allFiles.referencedAndUnreferenced().stream()
                        .filter(file -> !file.getReferences().isEmpty())
                        .map(StateStoreFile::toModel),
                filesWithNoReferences.stream().limit(maxUnreferencedFiles))
                .collect(toUnmodifiableList());
        return new AllReferencesToAllFiles(resultFiles, filesWithNoReferences.size() > maxUnreferencedFiles);
    }

    private void updateS3Files(Instant updateTime, FileReferenceTransaction transaction) throws StateStoreException {
        s3StateStoreFile.updateWithAttempts(10, files -> {
            transaction.apply(files, updateTime);
            return files;
        }, (ThrowingFileReferencesConditionCheck) transaction::validate);
    }

    /**
     * A conditional check for whether we can perform a given update to file references.
     */
    interface FileReferencesConditionCheck extends S3StateStoreDataFile.ConditionCheck<StateStoreFiles> {
    }

    /**
     * A conditional check for whether we can perform a given update to file references.
     */
    @FunctionalInterface
    interface ThrowingFileReferencesConditionCheck extends FileReferencesConditionCheck {
        default Optional<? extends StateStoreException> check(StateStoreFiles data) {
            try {
                throwingCheck(data);
                return Optional.empty();
            } catch (StateStoreException e) {
                return Optional.of(e);
            }
        }

        void throwingCheck(StateStoreFiles data) throws StateStoreException;
    }

    private S3RevisionId getCurrentFilesRevisionId() {
        return s3RevisionIdStore.getCurrentFilesRevisionId();
    }

    public void initialise() throws StateStoreException {
        S3RevisionId firstRevisionId = S3RevisionId.firstRevision(UUID.randomUUID().toString());
        String path = getFilesPath(firstRevisionId);
        LOGGER.debug("Writing initial empty file (revisionId = {}, path = {})", firstRevisionId, path);
        writeFiles(new StateStoreFiles(), path);
        s3RevisionIdStore.saveFirstFilesRevision(firstRevisionId);
    }

    @Override
    public boolean hasNoFiles() throws StateStoreException {
        S3RevisionId revisionId = getCurrentFilesRevisionId();
        if (revisionId == null) {
            return true;
        }
        try {
            return dataStore.isEmpty(getFilesPath(revisionId));
        } catch (IOException e) {
            throw new StateStoreException("Failed to load files", e);
        }
    }

    @Override
    public void clearFileData() throws StateStoreException {
        try {
            Path path = new Path(stateStorePath + "/files");
            path.getFileSystem(conf).delete(path, true);
            s3RevisionIdStore.deleteFilesRevision();
        } catch (IOException | RuntimeException e) {
            throw new StateStoreException("Failed deleting files file", e);
        }
    }

    private String getFilesPath(S3RevisionId revisionId) {
        return stateStorePath + "/files/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-files.arrow";
    }

    private void writeFiles(StateStoreFiles files, String path) throws StateStoreException {
        try {
            dataStore.saveFiles(path, files);
        } catch (IOException e) {
            throw new StateStoreException("Failed to save files", e);
        }
    }

    private StateStoreFiles readFiles(String path) throws StateStoreException {
        try {
            return dataStore.loadFiles(path);
        } catch (IOException e) {
            throw new StateStoreException("Failed to load files", e);
        }
    }

    public void fixFileUpdateTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private static String getPartitionIdAndFilename(FileReference fileReference) {
        return fileReference.getPartitionId() + DELIMITER + fileReference.getFilename();
    }

    /**
     * Builder to create a file reference store backed by S3.
     */
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
