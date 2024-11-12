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

import sleeper.clients.admin.AdminClientStatusStoreFactory;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.task.IngestTaskStatusStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class AdminClientStatusStoreHolder implements AdminClientStatusStoreFactory {

    private final Map<String, CompactionJobStatusStore> compactionJobStoreByInstance = new HashMap<>();
    private final Map<String, CompactionTaskStatusStore> compactionTaskStoreByInstance = new HashMap<>();
    private final Map<String, IngestJobStatusStore> ingestJobStoreByInstance = new HashMap<>();
    private final Map<String, IngestTaskStatusStore> ingestTaskStoreByInstance = new HashMap<>();
    private final Map<String, IngestBatcherStore> ingestBatcherStoreByInstance = new HashMap<>();

    public void setStore(String instanceId, CompactionJobStatusStore store) {
        compactionJobStoreByInstance.put(instanceId, store);
    }

    public void setStore(String instanceId, CompactionTaskStatusStore store) {
        compactionTaskStoreByInstance.put(instanceId, store);
    }

    public void setStore(String instanceId, IngestJobStatusStore store) {
        ingestJobStoreByInstance.put(instanceId, store);
    }

    public void setStore(String instanceId, IngestTaskStatusStore store) {
        ingestTaskStoreByInstance.put(instanceId, store);
    }

    public void setStore(String instanceId, IngestBatcherStore store) {
        ingestBatcherStoreByInstance.put(instanceId, store);
    }

    @Override
    public CompactionJobStatusStore loadCompactionJobStatusStore(InstanceProperties instanceProperties) {
        return Optional.ofNullable(compactionJobStoreByInstance.get(instanceProperties.get(ID)))
                .orElse(CompactionJobStatusStore.NONE);
    }

    @Override
    public CompactionTaskStatusStore loadCompactionTaskStatusStore(InstanceProperties instanceProperties) {
        return Optional.ofNullable(compactionTaskStoreByInstance.get(instanceProperties.get(ID)))
                .orElse(CompactionTaskStatusStore.NONE);
    }

    @Override
    public IngestJobStatusStore loadIngestJobStatusStore(InstanceProperties instanceProperties) {
        return Optional.ofNullable(ingestJobStoreByInstance.get(instanceProperties.get(ID)))
                .orElse(IngestJobStatusStore.NONE);
    }

    @Override
    public IngestTaskStatusStore loadIngestTaskStatusStore(InstanceProperties instanceProperties) {
        return Optional.ofNullable(ingestTaskStoreByInstance.get(instanceProperties.get(ID)))
                .orElse(IngestTaskStatusStore.NONE);
    }

    @Override
    public Optional<IngestBatcherStore> loadIngestBatcherStatusStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider) {
        return Optional.ofNullable(ingestBatcherStoreByInstance.get(properties.get(ID)));
    }
}
