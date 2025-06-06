/*
 * Copyright 2022-2025 Crown Copyright
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

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.storev2.IngestBatcherStoreFactory;
import sleeper.ingest.trackerv2.job.IngestJobTrackerFactory;
import sleeper.ingest.trackerv2.task.IngestTaskTrackerFactory;

import java.util.Optional;

public interface AdminClientTrackerFactory {

    CompactionJobTracker loadCompactionJobTracker(InstanceProperties instanceProperties);

    CompactionTaskTracker loadCompactionTaskTracker(InstanceProperties instanceProperties);

    IngestJobTracker loadIngestJobTracker(InstanceProperties instanceProperties);

    IngestTaskTracker loadIngestTaskTracker(InstanceProperties instanceProperties);

    Optional<IngestBatcherStore> loadIngestBatcherStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider);

    static AdminClientTrackerFactory from(DynamoDbClient dynamoClient) {
        return new AdminClientTrackerFactory() {
            @Override
            public CompactionJobTracker loadCompactionJobTracker(InstanceProperties instanceProperties) {
                return CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            }

            @Override
            public CompactionTaskTracker loadCompactionTaskTracker(InstanceProperties instanceProperties) {
                return CompactionTaskTrackerFactory.getTracker(dynamoClient, instanceProperties);
            }

            @Override
            public IngestJobTracker loadIngestJobTracker(InstanceProperties instanceProperties) {
                return IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            }

            @Override
            public IngestTaskTracker loadIngestTaskTracker(InstanceProperties instanceProperties) {
                return IngestTaskTrackerFactory.getTracker(dynamoClient, instanceProperties);
            }

            @Override
            public Optional<IngestBatcherStore> loadIngestBatcherStore(InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider) {
                return IngestBatcherStoreFactory.getStore(dynamoClient, properties, tablePropertiesProvider);
            }
        };
    }
}
