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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

class UpdateS3File {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateS3File.class);

    private UpdateS3File() {
    }

    static <T> void updateWithAttempts(Configuration conf, S3RevisionUtils revisionUtils, RevisionTrackedS3File<T> file,
                                       int attempts, Function<T, T> update, Function<T, String> condition)
            throws StateStoreException {
        Instant startTime = Instant.now();
        boolean success = false;
        int numberAttempts = 0;
        long totalTimeSleeping = 0L;
        while (numberAttempts < attempts) {
            numberAttempts++;
            S3RevisionId revisionId = file.getCurrentRevisionId(revisionUtils);
            String filePath = file.getPath(revisionId);
            T data;
            try {
                data = file.loadData(filePath);
                LOGGER.debug("Attempt number {}: reading {} (revisionId = {}, path = {})",
                        numberAttempts, file.getDescription(), revisionId, filePath);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to read {}; retrying", file.getDescription());
                totalTimeSleeping += sleep(numberAttempts);
                continue;
            }

            // Check condition
            String conditionCheck = condition.apply(data);
            if (!conditionCheck.isEmpty()) {
                throw new StateStoreException("Conditional check failed: " + conditionCheck);
            }

            // Apply update
            T updated = update.apply(data);
            LOGGER.debug("Applied update to {}", file.getDescription());

            // Attempt to write update
            S3RevisionId nextRevisionId = revisionUtils.getNextRevisionId(revisionId);
            String nextRevisionIdPath = file.getPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated {} (revisionId = {}, path = {})",
                        file.getDescription(), nextRevisionId, nextRevisionIdPath);
                file.writeData(updated, nextRevisionIdPath);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to write {}; retrying", file.getDescription());
                continue;
            }
            try {
                file.conditionalUpdateOfRevisionId(revisionUtils, revisionId, nextRevisionId);
                LOGGER.debug("Updated file information to revision {}", nextRevisionId);
                success = true;
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update {} failed with conditional check failure, deleting file {} and retrying ({}) ",
                        numberAttempts, file.getDescription(), nextRevisionIdPath, e.getMessage());
                Path path = new Path(nextRevisionIdPath);
                try {
                    path.getFileSystem(conf).delete(path, false);
                } catch (IOException e1) {
                    throw new StateStoreException("Failed to delete file after failing revision ID update", e1);
                }
                LOGGER.info("Deleted file {}", path);
                totalTimeSleeping += sleep(numberAttempts);
            }
        }
        if (success) {
            LOGGER.info("Succeeded updating {} with {} attempts; took {}; spent {} sleeping",
                    file.getDescription(), numberAttempts,
                    Duration.between(startTime, Instant.now()),
                    Duration.ofMillis(totalTimeSleeping));
        } else {
            LOGGER.error("Failed updating {} after too many attempts; {} attempts; took {}; spent {} sleeping",
                    file.getDescription(), numberAttempts,
                    Duration.between(startTime, Instant.now()),
                    Duration.ofMillis(totalTimeSleeping));
            throw new StateStoreException("Too many update attempts, failed after " + numberAttempts + " attempts");
        }
    }

    private static long sleep(int n) {
        // Implements exponential back-off with jitter, see
        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        int sleepTimeInSeconds = (int) Math.min(120, Math.pow(2.0, n + 1));
        long sleepTimeWithJitter = (long) (Math.random() * sleepTimeInSeconds * 1000L);
        LOGGER.debug("Sleeping for {} milliseconds", sleepTimeWithJitter);
        try {
            Thread.sleep(sleepTimeWithJitter);
        } catch (InterruptedException e) {
            // Do nothing
        }
        return sleepTimeWithJitter;
    }

}
