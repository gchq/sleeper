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

package sleeper.compaction.status.store.task;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_STATUS_STORE_ENABLED;

public class CompactionTaskStatusStoreFactory {

    private CompactionTaskStatusStoreFactory() {
    }

    public static CompactionTaskStatusStore getStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (properties.getBoolean(COMPACTION_STATUS_STORE_ENABLED)) {
            return new DynamoDBCompactionTaskStatusStore(dynamoDB, properties);
        } else {
            return CompactionTaskStatusStore.NONE;
        }
    }
}
