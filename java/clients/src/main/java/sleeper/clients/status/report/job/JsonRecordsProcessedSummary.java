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
package sleeper.clients.status.report.job;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.record.process.RecordsProcessedSummary;

public class JsonRecordsProcessedSummary {

    private JsonRecordsProcessedSummary() {
    }

    public static JsonSerializer<RecordsProcessedSummary> serializer() {
        return (summary, type, context) -> createSummaryJson(summary, context);
    }

    private static JsonElement createSummaryJson(RecordsProcessedSummary summary, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("recordsProcessed", context.serialize(summary.getRecordsProcessed()));
        jsonObject.add("runTime", context.serialize(summary.getRunTime()));
        jsonObject.addProperty("recordsReadPerSecond", summary.getRecordsReadPerSecond());
        jsonObject.addProperty("recordsWrittenPerSecond", summary.getRecordsWrittenPerSecond());
        return jsonObject;
    }
}
