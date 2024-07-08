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
package sleeper.compaction.job.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobCommittedEvent;
import sleeper.compaction.job.status.CompactionJobFinishedEvent;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.GetStateStoreByTableId;
import sleeper.core.statestore.StateStoreException;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;

public class CompactionJobCommitterOrSendToLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitterOrSendToLambda.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final GetStateStoreByTableId stateStoreProvider;
    private final CompactionJobStatusStore statusStore;
    private final CommitQueueSender jobCommitQueueSender;

    public CompactionJobCommitterOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, GetStateStoreByTableId stateStoreProvider,
            CompactionJobStatusStore statusStore, CommitQueueSender jobCommitQueueSender) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.statusStore = statusStore;
        this.jobCommitQueueSender = jobCommitQueueSender;
    }

    public void commit(CompactionJob job, CompactionJobFinishedEvent.Builder finishedBuilder) throws StateStoreException {
        boolean commitAsync = tablePropertiesProvider.getById(job.getTableId()).getBoolean(COMPACTION_JOB_COMMIT_ASYNC);
        CompactionJobFinishedEvent finishedEvent = finishedBuilder.committedBySeparateUpdate(commitAsync).build();
        if (commitAsync) {
            LOGGER.info("Sending compaction job {} to queue to be committed asynchronously", job.getId());
            jobCommitQueueSender.send(new CompactionJobCommitRequest(job,
                    finishedEvent.getTaskId(), finishedEvent.getJobRunId(), finishedEvent.getSummary()));
            statusStore.jobFinished(finishedEvent);
        } else {
            LOGGER.info("Committing compaction job {} inside compaction task", job.getId());
            CompactionJobCommitter.updateStateStoreSuccess(job, finishedEvent.getSummary().getRecordsWritten(), stateStoreProvider.getByTableId(job.getTableId()));
            statusStore.jobCommitted(CompactionJobCommittedEvent.compactionJobCommitted(job)
                    .jobRunId(finishedEvent.getJobRunId())
                    .taskId(finishedEvent.getTaskId())
                    .build());
            statusStore.jobFinished(finishedEvent);
            LOGGER.info("Successfully committed compaction job {} to table with ID {}", job.getId(), job.getTableId());
        }
    }

    public interface CommitQueueSender {
        void send(CompactionJobCommitRequest commitRequest);
    }
}
