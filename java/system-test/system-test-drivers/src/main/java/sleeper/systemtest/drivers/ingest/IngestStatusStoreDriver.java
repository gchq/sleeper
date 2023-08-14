/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;

public class IngestStatusStoreDriver {
    private final IngestJobStatusStore ingestJobStatusStore;
    private final IngestTaskStatusStore ingestTaskStatusStore;

    public IngestStatusStoreDriver(AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        this.ingestJobStatusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
        this.ingestTaskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
    }

    public void clearStores() {
        ingestJobStatusStore.clear();
        ingestTaskStatusStore.clear();
    }
}
