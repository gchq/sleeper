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
package sleeper.compaction.core.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;

public class CompactionJobCommitterOrSendToLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitterOrSendToLambda.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker tracker;
    private final CommitQueueSender jobCommitQueueSender;
    private final Supplier<Instant> timeSupplier;

    public CompactionJobCommitterOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider,
            CompactionJobTracker tracker, CommitQueueSender jobCommitQueueSender) {
        this(tablePropertiesProvider, stateStoreProvider, tracker, jobCommitQueueSender, Instant::now);
    }

    public CompactionJobCommitterOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider,
            CompactionJobTracker tracker, CommitQueueSender jobCommitQueueSender, Supplier<Instant> timeSupplier) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.tracker = tracker;
        this.jobCommitQueueSender = jobCommitQueueSender;
        this.timeSupplier = timeSupplier;
    }

    public void commit(CompactionJob job, CompactionJobFinishedEvent finishedEvent) {
        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        TableStatus table = tableProperties.getStatus();
        boolean commitAsync = tableProperties.getBoolean(COMPACTION_JOB_COMMIT_ASYNC);
        tracker.jobFinished(finishedEvent);
        if (commitAsync) {
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    job.createReplaceFileReferencesRequest(finishedEvent)));
            StateStoreCommitRequest request = StateStoreCommitRequest.create(table.getTableUniqueId(), transaction);
            LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
            jobCommitQueueSender.send(request);
            LOGGER.info("Sent compaction job {} to queue to be committed asynchronously to table {}", job.getId(), table);
        } else {
            LOGGER.debug("Committing compaction job {} inside compaction task", job.getId());
            stateStoreProvider.getStateStore(tableProperties).atomicallyReplaceFileReferencesWithNewOnes(
                    List.of(job.createReplaceFileReferencesRequest(finishedEvent)));
            tracker.jobCommitted(job.committedEventBuilder(timeSupplier.get())
                    .jobRunId(finishedEvent.getJobRunId())
                    .taskId(finishedEvent.getTaskId())
                    .build());
            LOGGER.info("Successfully committed compaction job {} to table {}", job.getId(), table);
        }
    }

    public interface CommitQueueSender {
        void send(StateStoreCommitRequest commitRequest);
    }
}
