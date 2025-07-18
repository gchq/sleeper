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

package sleeper.compaction.tracker.task;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;

import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;

public class CompactionTaskTrackerFactory {

    private CompactionTaskTrackerFactory() {
    }

    public static CompactionTaskTracker getTracker(DynamoDbClient dynamoDB, InstanceProperties properties) {
        if (properties.getBoolean(COMPACTION_TRACKER_ENABLED)) {
            return new DynamoDBCompactionTaskTracker(dynamoDB, properties);
        } else {
            return CompactionTaskTracker.NONE;
        }
    }
}
