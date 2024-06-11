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
package sleeper.ingest.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.job.commit.IngestJobCommitRequest;
import sleeper.ingest.job.commit.IngestJobCommitter;

/**
 * Commits the results of an ingest job by either sending a commit request to the state store committer lambda, or
 * performs the commit synchronously.
 */
public class IngestJobCommitterOrSendToLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestJobCommitterOrSendToLambda.class);

    private final TableCommitConfig tableCommitConfig;
    private final IngestJobCommitter jobCommitter;
    private final CommitQueueSender jobCommitQueueSender;

    public IngestJobCommitterOrSendToLambda(TableCommitConfig tableCommitConfig,
            IngestJobCommitter jobCommitter, CommitQueueSender jobCommitQueueSender) {
        this.tableCommitConfig = tableCommitConfig;
        this.jobCommitter = jobCommitter;
        this.jobCommitQueueSender = jobCommitQueueSender;
    }

    /**
     * Commits the results of an ingest job by either sending a commit request to the state store committer lambda, or
     * performs the commit synchronously.
     *
     * @param  commitRequest       the commit request
     * @throws StateStoreException if the synchronous commit failed to update the state store
     */
    public void commit(IngestJobCommitRequest commitRequest) throws StateStoreException {
        if (tableCommitConfig.shouldCommitAsync(commitRequest.getJob().getTableId())) {
            LOGGER.info("Sending ingest job {} to queue to be committed asynchronously", commitRequest.getJob().getId());
            jobCommitQueueSender.commit(commitRequest);
        } else {
            LOGGER.info("Committing ingest job {} inside ingest task", commitRequest.getJob().getId());
            jobCommitter.apply(commitRequest);
        }
    }

    /**
     * Checks how the table is configured to commit ingest jobs.
     */
    public interface TableCommitConfig {
        /**
         * Checks how the table is configured to commit ingest jobs.
         *
         * @param  tableId the table ID
         * @return         true if ingest jobs are to be committed asynchronously, false if otherwise
         */
        boolean shouldCommitAsync(String tableId);
    }

    /**
     * Commits the results of an ingest job asynchronously.
     */
    public interface CommitQueueSender {
        /**
         * Commits the results of an ingest job asynchronously.
         *
         * @param commitRequest the ingest job commit request
         */
        void commit(IngestJobCommitRequest commitRequest);
    }
}
