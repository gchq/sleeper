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
import sleeper.core.statestore.StateStoreProvider;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class CompactionJobDispatcher {

    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;
    private final ReadBatch readBatch;
    private final CompactionJobStatusStore statusStore;
    private final SendJob sendJob;

    public CompactionJobDispatcher(
            InstanceProperties instanceProperties, StateStoreProvider stateStoreProvider, ReadBatch readBatch,
            CompactionJobStatusStore statusStore, SendJob sendJob) {
        this.instanceProperties = instanceProperties;
        this.stateStoreProvider = stateStoreProvider;
        this.readBatch = readBatch;
        this.statusStore = statusStore;
        this.sendJob = sendJob;
    }

    public void dispatch(CompactionJobDispatchRequest request) {
        List<CompactionJob> batch = readBatch.readBatch(instanceProperties.get(DATA_BUCKET), request.getBatchKey());
        for (CompactionJob job : batch) {
            statusStore.jobCreated(job);
            sendJob.send(job);
        }
    }

    public interface ReadBatch {

        List<CompactionJob> readBatch(String bucketName, String key);
    }

    public interface SendJob {

        void send(CompactionJob job);
    }

}
