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

package sleeper.ingest.tracker.task;

import software.amazon.awssdk.services.dynamodb.AmazonDynamoDB;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;

import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;

public class IngestTaskTrackerFactory {

    private IngestTaskTrackerFactory() {
    }

    public static IngestTaskTracker getTracker(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (properties.getBoolean(INGEST_TRACKER_ENABLED)) {
            return new DynamoDBIngestTaskTracker(dynamoDB, properties);
        } else {
            return IngestTaskTracker.NONE;
        }
    }
}
