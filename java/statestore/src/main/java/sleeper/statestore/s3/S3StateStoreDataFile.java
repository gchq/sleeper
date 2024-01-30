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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.StateStoreException;

import java.time.Duration;
import java.time.Instant;
import java.util.function.DoubleSupplier;
import java.util.function.Function;

/**
 * This class holds the operations needed to save or load a file in the S3 state store, and is used when performing
 * updates to a file. These files store all the data held in the S3 state store.
 * <p>
 * Each file is tracked with revisions held separately, which can be used to derive the path of the file in S3. Each
 * file has a revision ID key which is used to reference the current revision ID for that file.
 * <p>
 * For each file, we need to be able to:
 *
 * <ul>
 *     <li>Derive the path where the file is stored in S3 from its revision ID</li>
 *     <li>Load data from a file at a certain path</li>
 *     <li>Write data to a file at a certain path</li>
 * </ul>
 * <p>
 * Each file contains a different type of data. This is stored in Parquet files, but loading and writing that data may
 * be done differently for each file.
 *
 * @param <T> The type of data held in the file
 */
class S3StateStoreDataFile<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StateStoreDataFile.class);

    private final DoubleSupplier randomJitterFraction;
    private final Waiter waiter;
    private final LoadS3RevisionId loadRevisionId;
    private final UpdateS3RevisionId updateRevisionId;
    private final S3StateStoreFileOperations<T> fileOperations;

    S3StateStoreDataFile(S3RevisionIdStore revisionStore, S3StateStoreFileOperations<T> fileOperations) {
        this(Math::random, Waiter.threadSleep(),
                revisionStore::getCurrentRevisionId, revisionStore::conditionalUpdateOfRevisionId,
                fileOperations);
    }

    S3StateStoreDataFile(
            DoubleSupplier randomJitterFraction, Waiter waiter,
            LoadS3RevisionId loadRevisionId, UpdateS3RevisionId updateRevisionId,
            S3StateStoreFileOperations<T> fileOperations) {
        this.randomJitterFraction = randomJitterFraction;
        this.waiter = waiter;
        this.loadRevisionId = loadRevisionId;
        this.updateRevisionId = updateRevisionId;
        this.fileOperations = fileOperations;
    }

    static <T> void updateWithAttempts(
            S3RevisionIdStore revisionStore, S3StateStoreFileOperations<T> storeFile,
            int attempts, Function<T, T> update, Function<T, String> condition)
            throws StateStoreException {
        updateWithAttempts(
                Math::random, Waiter.threadSleep(),
                revisionStore::getCurrentRevisionId, revisionStore::conditionalUpdateOfRevisionId,
                storeFile, attempts, update, condition);
    }

    static <T> void updateWithAttempts(
            DoubleSupplier randomJitterFraction, Waiter waiter,
            LoadS3RevisionId loadRevisionId, UpdateS3RevisionId updateRevisionId,
            S3StateStoreFileOperations<T> storeFile,
            int attempts, Function<T, T> update, Function<T, String> condition)
            throws StateStoreException {
    }

    void updateWithAttempts(
            int attempts, Function<T, T> update, Function<T, String> condition)
            throws StateStoreException {
        Instant startTime = Instant.now();
        boolean success = false;
        int numberAttempts = 0;
        long totalTimeSleeping = 0L;
        while (numberAttempts < attempts) {
            totalTimeSleeping += sleep(randomJitterFraction, waiter, numberAttempts);
            numberAttempts++;
            S3RevisionId revisionId = loadRevisionId.getCurrentRevisionId(fileOperations.getRevisionIdKey());
            String filePath = fileOperations.getPath(revisionId);
            T data;
            try {
                LOGGER.debug("Attempt number {}: reading {} (revisionId = {}, path = {})",
                        numberAttempts, fileOperations.getDescription(), revisionId, filePath);
                data = fileOperations.loadData(filePath);
            } catch (StateStoreException e) {
                LOGGER.error("Failed reading {}; retrying", fileOperations.getDescription(), e);
                continue;
            }

            // Check condition
            LOGGER.debug("Loaded {}, checking condition", fileOperations.getDescription());
            String conditionCheck = condition.apply(data);
            if (!conditionCheck.isEmpty()) {
                throw new StateStoreException("Conditional check failed: " + conditionCheck);
            }

            // Apply update
            LOGGER.debug("Condition met, updating {}", fileOperations.getDescription());
            T updated = update.apply(data);

            // Attempt to write update
            S3RevisionId nextRevisionId = revisionId.getNextRevisionId();
            String nextRevisionIdPath = fileOperations.getPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated {} (revisionId = {}, path = {})",
                        fileOperations.getDescription(), nextRevisionId, nextRevisionIdPath);
                fileOperations.writeData(updated, nextRevisionIdPath);
            } catch (StateStoreException e) {
                LOGGER.debug("Failed writing {}; retrying", fileOperations.getDescription(), e);
                continue;
            }
            try {
                updateRevisionId.conditionalUpdateOfRevisionId(fileOperations.getRevisionIdKey(), revisionId, nextRevisionId);
                LOGGER.debug("Updated file information to revision {}", nextRevisionId);
                success = true;
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update {} failed with conditional check failure, deleting file {} and retrying ({}) ",
                        numberAttempts, fileOperations.getDescription(), nextRevisionIdPath, e.getMessage());
                fileOperations.deleteFile(nextRevisionIdPath);
                LOGGER.info("Deleted file {}", nextRevisionIdPath);
            }
        }
        if (success) {
            LOGGER.info("Succeeded updating {} with {} attempts; took {}; spent {} sleeping",
                    fileOperations.getDescription(), numberAttempts,
                    Duration.between(startTime, Instant.now()),
                    Duration.ofMillis(totalTimeSleeping));
        } else {
            LOGGER.error("Failed updating {} after too many attempts; {} attempts; took {}; spent {} sleeping",
                    fileOperations.getDescription(), numberAttempts,
                    Duration.between(startTime, Instant.now()),
                    Duration.ofMillis(totalTimeSleeping));
            throw new StateStoreException("Too many update attempts, failed after " + numberAttempts + " attempts");
        }
    }

    private static long sleep(DoubleSupplier randomJitterFraction, Waiter waiter, int n) throws StateStoreException {
        if (n == 0) {
            return 0;
        }
        // Implements exponential back-off with jitter, see
        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        int sleepTimeInSeconds = (int) Math.min(120, Math.pow(2.0, n + 1));
        long sleepTimeWithJitter = (long) (randomJitterFraction.getAsDouble() * sleepTimeInSeconds * 1000L);
        LOGGER.debug("Sleeping for {} milliseconds", sleepTimeWithJitter);
        waiter.waitForMillis(sleepTimeWithJitter);
        return sleepTimeWithJitter;
    }

    interface Waiter {
        void waitForMillis(long milliseconds) throws StateStoreException;

        static Waiter threadSleep() {
            return milliseconds -> {
                try {
                    Thread.sleep(milliseconds);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StateStoreException("Interrupted while waiting to retry update", e);
                }
            };
        }
    }

    interface LoadS3RevisionId {
        S3RevisionId getCurrentRevisionId(String revisionIdKey);
    }

    interface UpdateS3RevisionId {
        void conditionalUpdateOfRevisionId(String revisionIdKey, S3RevisionId currentRevisionId, S3RevisionId newRevisionId);
    }

}
