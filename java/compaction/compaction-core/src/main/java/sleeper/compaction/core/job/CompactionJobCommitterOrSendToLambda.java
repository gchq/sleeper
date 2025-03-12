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

import sleeper.compaction.core.job.commit.CompactionCommitMessage;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ASYNC_BATCHING;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;

public class CompactionJobCommitterOrSendToLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobCommitterOrSendToLambda.class);

    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker tracker;
    private final CommitQueueSender jobCommitQueueSender;
    private final BatchedCommitQueueSender batchedCommitQueueSender;
    private final Supplier<Instant> timeSupplier;

    public CompactionJobCommitterOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider,
            CompactionJobTracker tracker, CommitQueueSender jobCommitQueueSender, BatchedCommitQueueSender batchedCommitQueueSender) {
        this(tablePropertiesProvider, stateStoreProvider, tracker, jobCommitQueueSender, batchedCommitQueueSender, Instant::now);
    }

    public CompactionJobCommitterOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider, CompactionJobTracker tracker,
            CommitQueueSender jobCommitQueueSender, BatchedCommitQueueSender batchedCommitQueueSender, Supplier<Instant> timeSupplier) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.tracker = tracker;
        this.jobCommitQueueSender = jobCommitQueueSender;
        this.batchedCommitQueueSender = batchedCommitQueueSender;
        this.timeSupplier = timeSupplier;
    }

    public void commit(CompactionJob job, CompactionJobFinishedEvent finishedEvent) {
        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        TableStatus table = tableProperties.getStatus();
        tracker.jobFinished(finishedEvent);
        ReplaceFileReferencesRequest.Builder requestBuilder = job.replaceFileReferencesRequestBuilder(finishedEvent.getRecordsProcessed().getRecordsWritten());
        if (tableProperties.getBoolean(COMPACTION_JOB_COMMIT_ASYNC)) {
            ReplaceFileReferencesRequest request = requestBuilder.taskId(finishedEvent.getTaskId()).jobRunId(finishedEvent.getJobRunId()).build();
            if (tableProperties.getBoolean(COMPACTION_JOB_ASYNC_BATCHING)) {
                CompactionCommitMessage message = new CompactionCommitMessage(table.getTableUniqueId(), request);
                LOGGER.debug("Sending asynchronous request to commit batcher: {}", message);
                batchedCommitQueueSender.send(message);
                LOGGER.info("Sent compaction job {} to batcher queue to be committed asynchronously to table {}", job.getId(), table);
            } else {
                StateStoreCommitRequest commitRequest = StateStoreCommitRequest.create(table.getTableUniqueId(),
                        new ReplaceFileReferencesTransaction(List.of(request)));
                LOGGER.debug("Sending asynchronous request to state store committer: {}", commitRequest);
                jobCommitQueueSender.send(commitRequest);
                LOGGER.info("Sent compaction job {} to state store queue to be committed asynchronously to table {}", job.getId(), table);
            }
        } else {
            LOGGER.debug("Committing compaction job {} inside compaction task", job.getId());
            new ReplaceFileReferencesTransaction(List.of(requestBuilder.build()))
                    .synchronousCommit(stateStoreProvider.getStateStore(tableProperties));
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

    public interface BatchedCommitQueueSender {
        void send(CompactionCommitMessage commitRequest);
    }
}
