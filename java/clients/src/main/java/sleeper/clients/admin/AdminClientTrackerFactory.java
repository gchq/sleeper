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

import sleeper.compaction.status.store.job.CompactionJobTrackerFactory;
import sleeper.compaction.status.store.task.CompactionTaskTrackerFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.store.IngestBatcherStoreFactory;
import sleeper.ingest.status.store.job.IngestJobTrackerFactory;
import sleeper.ingest.status.store.task.IngestTaskTrackerFactory;

import java.util.Optional;

public interface AdminClientTrackerFactory {

    CompactionJobTracker loadCompactionJobTracker(InstanceProperties instanceProperties);

    CompactionTaskTracker loadCompactionTaskTracker(InstanceProperties instanceProperties);

    IngestJobTracker loadIngestJobTracker(InstanceProperties instanceProperties);

    IngestTaskTracker loadIngestTaskTracker(InstanceProperties instanceProperties);

    Optional<IngestBatcherStore> loadIngestBatcherStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider);

    static AdminClientTrackerFactory from(AmazonDynamoDB dynamoDB) {
        return new AdminClientTrackerFactory() {
            @Override
            public CompactionJobTracker loadCompactionJobTracker(InstanceProperties instanceProperties) {
                return CompactionJobTrackerFactory.getTracker(dynamoDB, instanceProperties);
            }

            @Override
            public CompactionTaskTracker loadCompactionTaskTracker(InstanceProperties instanceProperties) {
                return CompactionTaskTrackerFactory.getTracker(dynamoDB, instanceProperties);
            }

            @Override
            public IngestJobTracker loadIngestJobTracker(InstanceProperties instanceProperties) {
                return IngestJobTrackerFactory.getTracker(dynamoDB, instanceProperties);
            }

            @Override
            public IngestTaskTracker loadIngestTaskTracker(InstanceProperties instanceProperties) {
                return IngestTaskTrackerFactory.getTracker(dynamoDB, instanceProperties);
            }

            @Override
            public Optional<IngestBatcherStore> loadIngestBatcherStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider) {
                return IngestBatcherStoreFactory.getStore(dynamoDB, properties, tablePropertiesProvider);
            }
        };
    }
}
