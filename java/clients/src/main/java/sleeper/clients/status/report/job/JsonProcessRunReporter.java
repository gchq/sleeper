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

package sleeper.clients.status.report.job;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.util.List;

public class JsonProcessRunReporter {
    private JsonProcessRunReporter() {
    }

    public static JsonSerializer<ProcessRuns> processRunsJsonSerializer() {
        return ((processRuns, type, context) -> createProcessRunsJson(processRuns, context));
    }

    private static JsonElement createProcessRunsJson(ProcessRuns runs, JsonSerializationContext context) {
        JsonArray jsonArray = new JsonArray();
        for (ProcessRun run : runs.getRunsLatestFirst()) {
            jsonArray.add(createProcessRunJson(run, context));
        }
        return jsonArray;
    }

    private static JsonElement createProcessRunJson(ProcessRun run, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("taskId", run.getTaskId());
        jsonObject.add("updates", createStatusUpdatesJson(run.getStatusUpdates(), context));
        return jsonObject;
    }

    private static JsonElement createStatusUpdatesJson(
            List<ProcessStatusUpdate> updates, JsonSerializationContext context) {
        JsonArray jsonArray = new JsonArray();
        for (ProcessStatusUpdate update : updates) {
            JsonObject jsonObject = context.serialize(update).getAsJsonObject();
            jsonObject.addProperty("type", update.getClass().getSimpleName());
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }
}
