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
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.compaction.core.job.status.CompactionJobCreatedEvent.compactionJobCreated;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_RETRY_DELAY_SECS;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_TIMEOUT_SECS;

public class CompactionJobDispatcher {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobDispatcher.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore statusStore;
    private final ReadBatch readBatch;
    private final SendJob sendJob;
    private final ReturnRequestToPendingQueue returnToPendingQueue;
    private final Supplier<Instant> timeSupplier;

    public CompactionJobDispatcher(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, CompactionJobStatusStore statusStore, ReadBatch readBatch,
            SendJob sendJob, ReturnRequestToPendingQueue returnToPendingQueue,
            Supplier<Instant> timeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.statusStore = statusStore;
        this.readBatch = readBatch;
        this.sendJob = sendJob;
        this.returnToPendingQueue = returnToPendingQueue;
        this.timeSupplier = timeSupplier;
    }

    public void dispatch(CompactionJobDispatchRequest request) {
        TableProperties tableProperties = tablePropertiesProvider.getById(request.getTableId());
        LOGGER.info("Received compaction batch for table {}, held at: {}",
                tableProperties.getStatus(), request.getBatchKey());
        List<CompactionJob> batch = readBatch.read(instanceProperties.get(DATA_BUCKET), request.getBatchKey());
        LOGGER.info("Read {} compaction jobs", batch.size());
        if (validateBatchIsValidToBeSent(batch, tableProperties)) {
            LOGGER.info("Validated input file assignments, sending {} jobs", batch.size());
            for (CompactionJob job : batch) {
                sendJob.send(job);
                statusStore.jobCreated(compactionJobCreated(job));
            }
            LOGGER.info("Sent {} jobs", batch.size());
        } else {
            LOGGER.info("Found file assignments not yet made");
            Instant expiryTime = request.getCreateTime().plus(
                    Duration.ofSeconds(tableProperties.getInt(COMPACTION_JOB_SEND_TIMEOUT_SECS)));
            if (timeSupplier.get().isAfter(expiryTime)) {
                throw new CompactionJobBatchExpiredException(request, expiryTime);
            }
            int delaySeconds = tableProperties.getInt(COMPACTION_JOB_SEND_RETRY_DELAY_SECS);
            returnToPendingQueue.sendWithDelay(request, delaySeconds);
            LOGGER.info("Returned compaction batch to pending queue with a delay of {} seconds, held at: {}",
                    delaySeconds, request.getBatchKey());
        }
    }

    private boolean validateBatchIsValidToBeSent(List<CompactionJob> batch, TableProperties tableProperties) {
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        return stateStore.isAssigned(batch.stream()
                .map(CompactionJob::createInputFileAssignmentsCheck)
                .toList());
    }

    @FunctionalInterface
    public interface ReadBatch {

        List<CompactionJob> read(String bucketName, String key);
    }

    @FunctionalInterface
    public interface SendJob {

        void send(CompactionJob job);
    }

    @FunctionalInterface
    public interface ReturnRequestToPendingQueue {
        void sendWithDelay(CompactionJobDispatchRequest request, int delaySeconds);
    }

}
