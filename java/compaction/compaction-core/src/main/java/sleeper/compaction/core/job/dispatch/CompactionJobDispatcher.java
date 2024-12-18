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
package sleeper.compaction.core.job.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.SplitIntoBatches;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_RETRY_DELAY_SECS;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_TIMEOUT_SECS;

public class CompactionJobDispatcher {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobDispatcher.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker tracker;
    private final ReadBatch readBatch;
    private final SendJobs sendJobs;
    private final int sendBatchSize;
    private final ReturnRequestToPendingQueue returnToPendingQueue;
    private final SendDeadLetter sendDeadLetter;
    private final Supplier<Instant> timeSupplier;

    public CompactionJobDispatcher(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, CompactionJobTracker tracker, ReadBatch readBatch,
            SendJobs sendJobs, int sendBatchSize,
            ReturnRequestToPendingQueue returnToPendingQueue, SendDeadLetter sendDeadLetter,
            Supplier<Instant> timeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.tracker = tracker;
        this.readBatch = readBatch;
        this.sendJobs = sendJobs;
        this.sendBatchSize = sendBatchSize;
        this.returnToPendingQueue = returnToPendingQueue;
        this.sendDeadLetter = sendDeadLetter;
        this.timeSupplier = timeSupplier;
    }

    public void dispatch(CompactionJobDispatchRequest request) {
        TableProperties tableProperties = tablePropertiesProvider.getById(request.getTableId());
        LOGGER.info("Received compaction batch for table {}, held at: {}",
                tableProperties.getStatus(), request.getBatchKey());
        List<CompactionJob> batch = readBatch.read(instanceProperties.get(DATA_BUCKET), request.getBatchKey());
        LOGGER.info("Read {} compaction jobs", batch.size());
        try {
            if (batchIsReadyToBeSent(tableProperties, batch)) {
                send(batch);
            } else {
                returnToQueueWithDelay(tableProperties, request);
            }
        } catch (FileReferenceAssignedToJobException | FileReferenceNotFoundException e) {
            LOGGER.error("Unexpected state found in state store, sending batch to dead letter queue", e);
            sendDeadLetter.send(request);
        }
    }

    private boolean batchIsReadyToBeSent(TableProperties tableProperties, List<CompactionJob> batch) {
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        return stateStore.isAssigned(batch.stream()
                .map(CompactionJob::createInputFileAssignmentsCheck)
                .toList());
    }

    private void send(List<CompactionJob> batch) {
        LOGGER.info("Validated input file assignments, sending {} jobs", batch.size());
        for (List<CompactionJob> toSend : SplitIntoBatches.splitListIntoBatchesOf(sendBatchSize, batch)) {
            sendJobs.send(toSend);
            toSend.forEach(job -> tracker.jobCreated(job.createCreatedEvent()));
        }
        LOGGER.info("Sent {} jobs", batch.size());
    }

    private void returnToQueueWithDelay(TableProperties tableProperties, CompactionJobDispatchRequest request) {
        LOGGER.info("Found file assignments not yet made");
        Instant expiryTime = request.getCreateTime().plus(
                Duration.ofSeconds(tableProperties.getInt(COMPACTION_JOB_SEND_TIMEOUT_SECS)));
        if (timeSupplier.get().isAfter(expiryTime)) {
            LOGGER.error("Found batch expired, sending to dead letter queue");
            sendDeadLetter.send(request);
            return;
        }
        int delaySeconds = tableProperties.getInt(COMPACTION_JOB_SEND_RETRY_DELAY_SECS);
        returnToPendingQueue.sendWithDelay(request, delaySeconds);
        LOGGER.info("Returned compaction batch to pending queue with a delay of {} seconds, held at: {}",
                delaySeconds, request.getBatchKey());
    }

    @FunctionalInterface
    public interface ReadBatch {

        List<CompactionJob> read(String bucketName, String key);
    }

    @FunctionalInterface
    public interface SendJobs {

        void send(List<CompactionJob> jobs);
    }

    @FunctionalInterface
    public interface ReturnRequestToPendingQueue {
        void sendWithDelay(CompactionJobDispatchRequest request, int delaySeconds);
    }

    @FunctionalInterface
    public interface SendDeadLetter {
        void send(CompactionJobDispatchRequest request);
    }

}
