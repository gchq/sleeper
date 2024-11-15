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

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_RETRY_DELAY_SECS;

public class CompactionJobDispatcher {

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final ReadBatch readBatch;
    private final CompactionJobStatusStore statusStore;
    private final SendJob sendJob;
    private final ReturnRequestToPendingQueue returnToPendingQueue;
    private final Supplier<Instant> timeSupplier;

    public CompactionJobDispatcher(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, ReadBatch readBatch,
            CompactionJobStatusStore statusStore, SendJob sendJob,
            ReturnRequestToPendingQueue returnToPendingQueue,
            Supplier<Instant> timeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.readBatch = readBatch;
        this.statusStore = statusStore;
        this.sendJob = sendJob;
        this.returnToPendingQueue = returnToPendingQueue;
        this.timeSupplier = timeSupplier;
    }

    public void dispatch(CompactionJobDispatchRequest request) throws StateStoreException {
        List<CompactionJob> batch = readBatch.readBatch(instanceProperties.get(DATA_BUCKET), request.getBatchKey());
        if (validateBatchIsValidToBeSent(batch, request.getTableId())) {
            for (CompactionJob job : batch) {
                statusStore.jobCreated(job);
                sendJob.send(job);
            }
        } else {
            if (timeSupplier.get().isAfter(request.getExpiryTime())) {
                throw new CompactionJobBatchExpiredException(request);
            }
            TableProperties tableProperties = tablePropertiesProvider.getById(request.getTableId());
            returnToPendingQueue.returnRequest(request, tableProperties.getInt(COMPACTION_JOB_SEND_RETRY_DELAY_SECS));
        }
    }

    private boolean validateBatchIsValidToBeSent(List<CompactionJob> batch, String tableId) throws StateStoreException {
        StateStore stateStore = stateStoreProvider.getStateStore(tablePropertiesProvider.getById(tableId));
        return stateStore.isAssigned(batch.stream()
                .map(CompactionJob::createInputFileAssignmentsCheck)
                .toList());
    }

    public interface ReadBatch {

        List<CompactionJob> readBatch(String bucketName, String key);
    }

    public interface SendJob {

        void send(CompactionJob job);
    }

    public interface ReturnRequestToPendingQueue {
        void returnRequest(CompactionJobDispatchRequest request, int delaySeconds);
    }

}
