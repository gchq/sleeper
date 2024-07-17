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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.core.util.LoggedDuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Saves or loads files that hold the state in the S3 state store. Used when performing updates to one of these data
 * files. These files store all the data held in the S3 state store.
 * <p>
 * Each file is tracked with revisions held separately, which can be used to derive the path of the file in S3. Each
 * file has a revision ID key which is used to reference the current revision ID for that file.
 * <p>
 * For each file, we need to be able to:
 * <ul>
 * <li>Derive the path where the file is stored in S3 from its revision ID</li>
 * <li>Load data from a file at a certain path</li>
 * <li>Write data to a file at a certain path</li>
 * </ul>
 * Each file contains a different type of data. This is stored in Parquet files, but loading and writing that data may
 * be done differently for each file.
 *
 * @param <T> The type of data held in the file
 */
class S3StateStoreDataFile<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StateStoreDataFile.class);

    public static final WaitRange RETRY_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(4, 120);

    private final String description;
    private final String revisionIdKey;
    private final LoadS3RevisionId loadRevisionId;
    private final UpdateS3RevisionId updateRevisionId;
    private final Function<S3RevisionId, String> buildPathFromRevisionId;
    private final LoadData<T> loadData;
    private final WriteData<T> writeData;
    private final DeleteFile deleteFile;
    private final ExponentialBackoffWithJitter retryBackoff;

    private S3StateStoreDataFile(Builder<T> builder) {
        description = Objects.requireNonNull(builder.description, "description must not be null");
        revisionIdKey = Objects.requireNonNull(builder.revisionIdKey, "revisionIdKey must not be null");
        loadRevisionId = Objects.requireNonNull(builder.loadRevisionId, "loadRevisionId must not be null");
        updateRevisionId = Objects.requireNonNull(builder.updateRevisionId, "updateRevisionId must not be null");
        buildPathFromRevisionId = Objects.requireNonNull(builder.buildPathFromRevisionId, "buildPathFromRevisionId must not be null");
        loadData = Objects.requireNonNull(builder.loadData, "loadData must not be null");
        writeData = Objects.requireNonNull(builder.writeData, "writeData must not be null");
        deleteFile = Objects.requireNonNull(builder.deleteFile, "deleteFile must not be null");
        retryBackoff = Objects.requireNonNull(builder.retryBackoff, "retryBackoff must not be null");
    }

    static Builder<?> builder() {
        return new Builder<>();
    }

    static <T> S3StateStoreDataFile.ConditionCheck<T> conditionCheckFor(Function<T, String> condition) {
        return files -> {
            String result = condition.apply(files);
            if (!result.isEmpty()) {
                return Optional.of(new StateStoreException(result));
            }
            return Optional.empty();
        };
    }

    void updateWithAttempts(
            int attempts, Function<T, T> update, ConditionCheck<T> condition) throws StateStoreException {
        Instant startTime = Instant.now();
        int successAttempt = -1;
        long totalTimeSleeping = 0L;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                totalTimeSleeping += retryBackoff.waitBeforeAttempt(attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StateStoreException("Interrupted waiting for retry", e);
            }
            S3RevisionId revisionId = loadRevisionId.getCurrentRevisionId(revisionIdKey);
            String filePath = buildPathFromRevisionId.apply(revisionId);
            T data;
            try {
                LOGGER.debug("Attempt number {}: reading {} (revisionId = {}, path = {})",
                        attempt, description, revisionId, filePath);
                data = loadData.load(filePath);
            } catch (StateStoreException e) {
                LOGGER.error("Failed reading {}; retrying", description, e);
                continue;
            }

            // Check condition
            LOGGER.debug("Loaded {}, checking condition", description);
            StateStoreException exception = condition.check(data).orElse(null);
            if (exception != null) {
                throw exception;
            }

            // Apply update
            LOGGER.debug("Condition met, updating {}", description);
            T updated = update.apply(data);

            // Attempt to write update
            S3RevisionId nextRevisionId = revisionId.getNextRevisionId();
            String nextRevisionIdPath = buildPathFromRevisionId.apply(nextRevisionId);
            try {
                LOGGER.debug("Writing updated {} (revisionId = {}, path = {})",
                        description, nextRevisionId, nextRevisionIdPath);
                writeData.write(updated, nextRevisionIdPath);
            } catch (StateStoreException e) {
                LOGGER.debug("Failed writing {}; retrying", description, e);
                continue;
            }
            try {
                updateRevisionId.conditionalUpdateOfRevisionId(revisionIdKey, revisionId, nextRevisionId);
                LOGGER.debug("Updated {} to revision {}", description, nextRevisionId);
                successAttempt = attempt;
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update {} failed with conditional check failure, deleting file {} and retrying ({}) ",
                        attempt, description, nextRevisionIdPath, e.getMessage());
                deleteFile.delete(nextRevisionIdPath);
                LOGGER.info("Deleted file {}", nextRevisionIdPath);
            }
        }
        if (successAttempt > 0) {
            LOGGER.info("Succeeded updating {} with {} attempts; took {}; spent {} sleeping",
                    description, successAttempt,
                    LoggedDuration.withShortOutput(startTime, Instant.now()),
                    LoggedDuration.withShortOutput(Duration.ofMillis(totalTimeSleeping)));
        } else {
            LOGGER.error("Failed updating {} after too many attempts; {} attempts; took {}; spent {} sleeping",
                    description, attempts,
                    LoggedDuration.withShortOutput(startTime, Instant.now()),
                    LoggedDuration.withShortOutput(Duration.ofMillis(totalTimeSleeping)));
            throw new StateStoreException("Too many update attempts, failed after " + attempts + " attempts");
        }
    }

    /**
     * Builder to create an object to save/load state in the S3 state store.
     *
     * @param <T> type of the state to save/load
     */
    static final class Builder<T> {
        private String description;
        private String revisionIdKey;
        private LoadS3RevisionId loadRevisionId;
        private UpdateS3RevisionId updateRevisionId;
        private Function<S3RevisionId, String> buildPathFromRevisionId;
        private LoadData<T> loadData;
        private WriteData<T> writeData;
        private DeleteFile deleteFile;
        private ExponentialBackoffWithJitter retryBackoff = new ExponentialBackoffWithJitter(RETRY_WAIT_RANGE);

        private Builder() {
        }

        Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        Builder<T> revisionIdKey(String revisionIdKey) {
            this.revisionIdKey = revisionIdKey;
            return this;
        }

        Builder<T> revisionStore(S3RevisionIdStore revisionStore) {
            return loadRevisionId(revisionStore::getCurrentRevisionId)
                    .updateRevisionId(revisionStore::conditionalUpdateOfRevisionId);
        }

        Builder<T> loadRevisionId(LoadS3RevisionId loadRevisionId) {
            this.loadRevisionId = loadRevisionId;
            return this;
        }

        Builder<T> updateRevisionId(UpdateS3RevisionId updateRevisionId) {
            this.updateRevisionId = updateRevisionId;
            return this;
        }

        Builder<T> buildPathFromRevisionId(Function<S3RevisionId, String> buildPathFromRevisionId) {
            this.buildPathFromRevisionId = buildPathFromRevisionId;
            return this;
        }

        <N> Builder<N> loadAndWriteData(LoadData<N> loadData, WriteData<N> writeData) {
            this.loadData = (LoadData<T>) loadData;
            this.writeData = (WriteData<T>) writeData;
            return (Builder<N>) this;
        }

        Builder<T> hadoopConf(Configuration conf) {
            return deleteFile(DeleteFile.withHadoop(conf));
        }

        Builder<T> deleteFile(DeleteFile deleteFile) {
            this.deleteFile = deleteFile;
            return this;
        }

        Builder<T> retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        S3StateStoreDataFile<T> build() {
            return new S3StateStoreDataFile<>(this);
        }
    }

    /**
     * Loads the state from a file.
     *
     * @param <T> type of the state to load
     */
    interface LoadData<T> {
        T load(String path) throws StateStoreException;
    }

    /**
     * Writes the state to a file.
     *
     * @param <T> type of the state to write
     */
    interface WriteData<T> {
        void write(T data, String path) throws StateStoreException;
    }

    /**
     * Deletes a file holding state store state.
     */
    interface DeleteFile {
        void delete(String path) throws StateStoreException;

        static DeleteFile withHadoop(Configuration conf) {
            return pathStr -> {
                Path path = new Path(pathStr);
                try {
                    path.getFileSystem(conf).delete(path, false);
                } catch (IOException e1) {
                    throw new StateStoreException("Failed to delete file after failing revision ID update", e1);
                }
            };
        }
    }

    /**
     * Loads the current revision ID from the revision index.
     */
    interface LoadS3RevisionId {
        S3RevisionId getCurrentRevisionId(String revisionIdKey);
    }

    /**
     * Updates the revision ID in the revision index.
     */
    interface UpdateS3RevisionId {
        void conditionalUpdateOfRevisionId(String revisionIdKey, S3RevisionId currentRevisionId, S3RevisionId newRevisionId);
    }

    /**
     * A conditional check for whether we can perform some update to the state.
     *
     * @param <T> type of the state being updated
     */
    @FunctionalInterface
    interface ConditionCheck<T> {
        Optional<? extends StateStoreException> check(T data);
    }
}
