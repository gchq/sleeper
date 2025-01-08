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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRuns;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.util.List;
import java.util.function.Function;

public class JsonJobRunReporter {
    private JsonJobRunReporter() {
    }

    public static JsonSerializer<JobRuns> processRunsJsonSerializer(Function<JobStatusUpdate, Object> getType) {
        return ((processRuns, type, context) -> createProcessRunsJson(processRuns, context, getType));
    }

    private static JsonElement createProcessRunsJson(
            JobRuns runs, JsonSerializationContext context, Function<JobStatusUpdate, Object> getType) {
        JsonArray jsonArray = new JsonArray();
        for (JobRun run : runs.getRunsLatestFirst()) {
            jsonArray.add(createProcessRunJson(run, context, getType));
        }
        return jsonArray;
    }

    private static JsonElement createProcessRunJson(
            JobRun run, JsonSerializationContext context, Function<JobStatusUpdate, Object> getType) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("taskId", run.getTaskId());
        jsonObject.add("updates", createStatusUpdatesJson(run.getStatusUpdates(), context, getType));
        return jsonObject;
    }

    private static JsonElement createStatusUpdatesJson(
            List<JobStatusUpdate> updates, JsonSerializationContext context, Function<JobStatusUpdate, Object> getType) {
        JsonArray jsonArray = new JsonArray();
        for (JobStatusUpdate update : updates) {
            JsonObject jsonObject = context.serialize(update).getAsJsonObject();
            jsonObject.add("type", context.serialize(getType.apply(update)));
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }
}
