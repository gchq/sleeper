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
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;

import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;

public class CompactionJobCommitterOrSendToLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitterOrSendToLambda.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore statusStore;
    private final CommitQueueSender jobCommitQueueSender;
    private final Supplier<Instant> timeSupplier;

    public CompactionJobCommitterOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider,
            CompactionJobStatusStore statusStore, CommitQueueSender jobCommitQueueSender) {
        this(tablePropertiesProvider, stateStoreProvider, statusStore, jobCommitQueueSender, Instant::now);
    }

    public CompactionJobCommitterOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider,
            CompactionJobStatusStore statusStore, CommitQueueSender jobCommitQueueSender, Supplier<Instant> timeSupplier) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.statusStore = statusStore;
        this.jobCommitQueueSender = jobCommitQueueSender;
        this.timeSupplier = timeSupplier;
    }

    public void commit(CompactionJob job, CompactionJobFinishedEvent finishedEvent) throws StateStoreException {
        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        TableStatus table = tableProperties.getStatus();
        boolean commitAsync = tableProperties.getBoolean(COMPACTION_JOB_COMMIT_ASYNC);
        statusStore.jobFinished(finishedEvent);
        if (commitAsync) {
            CompactionJobCommitRequest request = new CompactionJobCommitRequest(job,
                    finishedEvent.getTaskId(), finishedEvent.getJobRunId(), finishedEvent.getSummary());
            LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
            jobCommitQueueSender.send(request);
            LOGGER.info("Sent compaction job {} to queue to be committed asynchronously to table {}", job.getId(), table);
        } else {
            LOGGER.debug("Committing compaction job {} inside compaction task", job.getId());
            CompactionJobCommitter.updateStateStoreSuccess(job,
                    finishedEvent.getSummary().getRecordsWritten(),
                    stateStoreProvider.getStateStore(tableProperties));
            statusStore.jobCommitted(CompactionJobCommittedEvent.compactionJobCommitted(job, timeSupplier.get())
                    .jobRunId(finishedEvent.getJobRunId())
                    .taskId(finishedEvent.getTaskId())
                    .build());
            LOGGER.info("Successfully committed compaction job {} to table {}", job.getId(), table);
        }
    }

    public interface CommitQueueSender {
        void send(CompactionJobCommitRequest commitRequest);
    }
}
