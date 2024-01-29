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
            S3RevisionStore revisionStore, RevisionTrackedS3FileType<T> fileType,
            int attempts, Function<T, T> update, Function<T, String> condition)
            throws StateStoreException {
        updateWithAttemptsAndCondition(
                Math::random, Waiter.threadSleep(),
                revisionStore, fileType, attempts, update, data -> {
                    String conditionCheck = condition.apply(data);
                    if (!conditionCheck.isEmpty()) {
                        throw new StateStoreException("Conditional check failed: " + conditionCheck);
                    }
                });
    }

    static <T> void updateWithAttemptsAndCondition(
            S3RevisionStore revisionStore, RevisionTrackedS3FileType<T> fileType,
            int attempts, Function<T, T> update, ConditionCheck<T> condition)
            throws StateStoreException {
        updateWithAttemptsAndCondition(
                Math::random, Waiter.threadSleep(),
                revisionStore, fileType, attempts, update, condition);
    }

    static <T> void updateWithAttempts(
            DoubleSupplier randomJitterFraction, Waiter waiter,
            RevisionStore revisionStore, RevisionTrackedS3FileType<T> fileType,
            int attempts, Function<T, T> update, Function<T, String> condition)
            throws StateStoreException {
        updateWithAttemptsAndCondition(randomJitterFraction, waiter, revisionStore, fileType, attempts, update, data -> {
            String conditionCheck = condition.apply(data);
            if (!conditionCheck.isEmpty()) {
                throw new StateStoreException("Conditional check failed: " + conditionCheck);
            }
        });
    }

    static <T> void updateWithAttemptsAndCondition(
            DoubleSupplier randomJitterFraction, Waiter waiter,
            RevisionStore revisionStore, RevisionTrackedS3FileType<T> fileType,
            int attempts, Function<T, T> update, ConditionCheck<T> condition)
            throws StateStoreException {
        Instant startTime = Instant.now();
        boolean success = false;
        int numberAttempts = 0;
        long totalTimeSleeping = 0L;
        while (numberAttempts < attempts) {
            totalTimeSleeping += sleep(randomJitterFraction, waiter, numberAttempts);
            numberAttempts++;
            S3RevisionId revisionId = revisionStore.getCurrentRevisionId(fileType.getRevisionIdKey());
            String filePath = fileType.getPath(revisionId);
            T data;
            try {
                LOGGER.debug("Attempt number {}: reading {} (revisionId = {}, path = {})",
                        numberAttempts, fileType.getDescription(), revisionId, filePath);
                data = fileType.loadData(filePath);
            } catch (StateStoreException e) {
                LOGGER.error("Failed reading {}; retrying", fileType.getDescription(), e);
                continue;
            }

            // Check condition
            LOGGER.debug("Loaded {}, checking condition", fileType.getDescription());
            condition.check(data);

            // Apply update
            LOGGER.debug("Condition met, updating {}", fileType.getDescription());
            T updated = update.apply(data);

            // Attempt to write update
            S3RevisionId nextRevisionId = revisionId.getNextRevisionId();
            String nextRevisionIdPath = fileType.getPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated {} (revisionId = {}, path = {})",
                        fileType.getDescription(), nextRevisionId, nextRevisionIdPath);
                fileType.writeData(updated, nextRevisionIdPath);
            } catch (StateStoreException e) {
                LOGGER.debug("Failed writing {}; retrying", fileType.getDescription(), e);
                continue;
            }
            try {
                revisionStore.conditionalUpdateOfRevisionId(fileType.getRevisionIdKey(), revisionId, nextRevisionId);
                LOGGER.debug("Updated file information to revision {}", nextRevisionId);
                success = true;
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update {} failed with conditional check failure, deleting file {} and retrying ({}) ",
                        numberAttempts, fileType.getDescription(), nextRevisionIdPath, e.getMessage());
                fileType.deleteFile(nextRevisionIdPath);
                LOGGER.info("Deleted file {}", nextRevisionIdPath);
            }
        }
        if (success) {
            LOGGER.info("Succeeded updating {} with {} attempts; took {}; spent {} sleeping",
                    fileType.getDescription(), numberAttempts,
                    Duration.between(startTime, Instant.now()),
                    Duration.ofMillis(totalTimeSleeping));
        } else {
            LOGGER.error("Failed updating {} after too many attempts; {} attempts; took {}; spent {} sleeping",
                    fileType.getDescription(), numberAttempts,
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

    public interface ConditionCheck<T> {
        void check(T files) throws StateStoreException;
    }
}
