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

package sleeper.ingest.tracker.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;

public class IngestJobTrackerFactory {

    private IngestJobTrackerFactory() {
    }

    public static IngestJobTracker getTracker(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        return getTracker(dynamoDB, properties, Instant::now);
    }

    public static IngestJobTracker getTracker(AmazonDynamoDB dynamoDB, InstanceProperties properties, Supplier<Instant> getTimeNow) {
        if (properties.getBoolean(INGEST_TRACKER_ENABLED)) {
            return new DynamoDBIngestJobTracker(dynamoDB, properties, getTimeNow);
        } else {
            return IngestJobTracker.NONE;
        }
    }
}
