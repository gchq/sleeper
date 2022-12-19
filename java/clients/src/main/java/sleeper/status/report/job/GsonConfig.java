/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report.job;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;

public class GsonConfig {

    private GsonConfig() {
    }

    public static GsonBuilder standardBuilder() {
        return new GsonBuilder().serializeSpecialFloatingPointValues()
                .registerTypeAdapter(Instant.class, instantJsonSerializer())
                .registerTypeAdapter(Duration.class, durationSerializer())
                .registerTypeAdapter(RecordsProcessedSummary.class, JsonRecordsProcessedSummary.serializer())
                .setPrettyPrinting();
    }

    private static JsonSerializer<Instant> instantJsonSerializer() {
        return (instant, type, context) -> new JsonPrimitive(instant.toString());
    }

    private static JsonSerializer<Duration> durationSerializer() {
        return (duration, type, context) -> new JsonPrimitive(duration.toMillis() / 1000.0);
    }

}
