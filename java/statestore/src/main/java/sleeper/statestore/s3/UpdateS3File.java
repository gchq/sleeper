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

class UpdateS3File {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateS3File.class);

    private UpdateS3File() {
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
        Instant startTime = Instant.now();
        boolean success = false;
        int numberAttempts = 0;
        long totalTimeSleeping = 0L;
        while (numberAttempts < attempts) {
            totalTimeSleeping += sleep(randomJitterFraction, waiter, numberAttempts);
            numberAttempts++;
            S3RevisionId revisionId = loadRevisionId.getCurrentRevisionId(storeFile.getRevisionIdKey());
            String filePath = storeFile.getPath(revisionId);
            T data;
            try {
                LOGGER.debug("Attempt number {}: reading {} (revisionId = {}, path = {})",
                        numberAttempts, storeFile.getDescription(), revisionId, filePath);
                data = storeFile.loadData(filePath);
            } catch (StateStoreException e) {
                LOGGER.error("Failed reading {}; retrying", storeFile.getDescription(), e);
                continue;
            }

            // Check condition
            LOGGER.debug("Loaded {}, checking condition", storeFile.getDescription());
            String conditionCheck = condition.apply(data);
            if (!conditionCheck.isEmpty()) {
                throw new StateStoreException("Conditional check failed: " + conditionCheck);
            }

            // Apply update
            LOGGER.debug("Condition met, updating {}", storeFile.getDescription());
            T updated = update.apply(data);

            // Attempt to write update
            S3RevisionId nextRevisionId = revisionId.getNextRevisionId();
            String nextRevisionIdPath = storeFile.getPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated {} (revisionId = {}, path = {})",
                        storeFile.getDescription(), nextRevisionId, nextRevisionIdPath);
                storeFile.writeData(updated, nextRevisionIdPath);
            } catch (StateStoreException e) {
                LOGGER.debug("Failed writing {}; retrying", storeFile.getDescription(), e);
                continue;
            }
            try {
                updateRevisionId.conditionalUpdateOfRevisionId(storeFile.getRevisionIdKey(), revisionId, nextRevisionId);
                LOGGER.debug("Updated file information to revision {}", nextRevisionId);
                success = true;
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update {} failed with conditional check failure, deleting file {} and retrying ({}) ",
                        numberAttempts, storeFile.getDescription(), nextRevisionIdPath, e.getMessage());
                storeFile.deleteFile(nextRevisionIdPath);
                LOGGER.info("Deleted file {}", nextRevisionIdPath);
            }
        }
        if (success) {
            LOGGER.info("Succeeded updating {} with {} attempts; took {}; spent {} sleeping",
                    storeFile.getDescription(), numberAttempts,
                    Duration.between(startTime, Instant.now()),
                    Duration.ofMillis(totalTimeSleeping));
        } else {
            LOGGER.error("Failed updating {} after too many attempts; {} attempts; took {}; spent {} sleeping",
                    storeFile.getDescription(), numberAttempts,
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
