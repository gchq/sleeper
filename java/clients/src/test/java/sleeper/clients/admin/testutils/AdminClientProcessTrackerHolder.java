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
package sleeper.clients.admin.testutils;

import sleeper.clients.admin.AdminClientTrackerFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.task.IngestTaskTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class AdminClientProcessTrackerHolder implements AdminClientTrackerFactory {

    private final Map<String, CompactionJobTracker> compactionJobTrackerByInstance = new HashMap<>();
    private final Map<String, CompactionTaskTracker> compactionTaskTrackerByInstance = new HashMap<>();
    private final Map<String, IngestJobStatusStore> ingestJobStoreByInstance = new HashMap<>();
    private final Map<String, IngestTaskTracker> ingestTaskStoreByInstance = new HashMap<>();
    private final Map<String, IngestBatcherStore> ingestBatcherStoreByInstance = new HashMap<>();

    public void setTracker(String instanceId, CompactionJobTracker tracker) {
        compactionJobTrackerByInstance.put(instanceId, tracker);
    }

    public void setTracker(String instanceId, CompactionTaskTracker tracker) {
        compactionTaskTrackerByInstance.put(instanceId, tracker);
    }

    public void setTracker(String instanceId, IngestJobStatusStore tracker) {
        ingestJobStoreByInstance.put(instanceId, tracker);
    }

    public void setTracker(String instanceId, IngestTaskTracker tracker) {
        ingestTaskStoreByInstance.put(instanceId, tracker);
    }

    public void setBatcherStore(String instanceId, IngestBatcherStore store) {
        ingestBatcherStoreByInstance.put(instanceId, store);
    }

    @Override
    public CompactionJobTracker loadCompactionJobTracker(InstanceProperties instanceProperties) {
        return Optional.ofNullable(compactionJobTrackerByInstance.get(instanceProperties.get(ID)))
                .orElse(CompactionJobTracker.NONE);
    }

    @Override
    public CompactionTaskTracker loadCompactionTaskTracker(InstanceProperties instanceProperties) {
        return Optional.ofNullable(compactionTaskTrackerByInstance.get(instanceProperties.get(ID)))
                .orElse(CompactionTaskTracker.NONE);
    }

    @Override
    public IngestJobStatusStore loadIngestJobStatusStore(InstanceProperties instanceProperties) {
        return Optional.ofNullable(ingestJobStoreByInstance.get(instanceProperties.get(ID)))
                .orElse(IngestJobStatusStore.NONE);
    }

    @Override
    public IngestTaskTracker loadIngestTaskTracker(InstanceProperties instanceProperties) {
        return Optional.ofNullable(ingestTaskStoreByInstance.get(instanceProperties.get(ID)))
                .orElse(IngestTaskTracker.NONE);
    }

    @Override
    public Optional<IngestBatcherStore> loadIngestBatcherStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider) {
        return Optional.ofNullable(ingestBatcherStoreByInstance.get(properties.get(ID)));
    }
}
