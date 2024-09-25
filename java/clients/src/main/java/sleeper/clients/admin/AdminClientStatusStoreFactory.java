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
package sleeper.clients.admin;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.IngestBatcherStoreFactory;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;

import java.util.Optional;

public interface AdminClientStatusStoreFactory {

    CompactionJobStatusStore loadCompactionJobStatusStore(InstanceProperties instanceProperties);

    CompactionTaskStatusStore loadCompactionTaskStatusStore(InstanceProperties instanceProperties);

    IngestJobStatusStore loadIngestJobStatusStore(InstanceProperties instanceProperties);

    IngestTaskStatusStore loadIngestTaskStatusStore(InstanceProperties instanceProperties);

    Optional<IngestBatcherStore> loadIngestBatcherStatusStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider);

    static AdminClientStatusStoreFactory from(AmazonDynamoDB dynamoDB) {
        return new AdminClientStatusStoreFactory() {
            @Override
            public CompactionJobStatusStore loadCompactionJobStatusStore(InstanceProperties instanceProperties) {
                return CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
            }

            @Override
            public CompactionTaskStatusStore loadCompactionTaskStatusStore(InstanceProperties instanceProperties) {
                return CompactionTaskStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
            }

            @Override
            public IngestJobStatusStore loadIngestJobStatusStore(InstanceProperties instanceProperties) {
                return IngestJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
            }

            @Override
            public IngestTaskStatusStore loadIngestTaskStatusStore(InstanceProperties instanceProperties) {
                return IngestTaskStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
            }

            @Override
            public Optional<IngestBatcherStore> loadIngestBatcherStatusStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider) {
                return IngestBatcherStoreFactory.getStore(dynamoDB, properties, tablePropertiesProvider);
            }
        };
    }
}
