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

package sleeper.clients.report.job;

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

/**
 * A GSON plugin to write JSON for reports, on runs of jobs held in a job tracker. This must be used in combination with
 * other GSON plugins for the specific type of jobs being processed.
 */
public class JsonJobRunReporter {
    private JsonJobRunReporter() {
    }

    /**
     * Creates a GSON serialiser for job runs. Handles {@link JobRuns} objects. Needs some handling specific to the
     * type of jobs being processed.
     *
     * @param  getType a function to retreive the type of a status update for a job
     * @return         the GSON serialiser
     */
    public static JsonSerializer<JobRuns> jobRunsJsonSerializer(Function<JobStatusUpdate, Object> getType) {
        return (processRuns, type, context) -> createJobRunsJson(processRuns, context, getType);
    }

    private static JsonElement createJobRunsJson(
            JobRuns runs, JsonSerializationContext context, Function<JobStatusUpdate, Object> getType) {
        JsonArray jsonArray = new JsonArray();
        for (JobRun run : runs.getRunsLatestFirst()) {
            jsonArray.add(createJobRunJson(run, context, getType));
        }
        return jsonArray;
    }

    private static JsonElement createJobRunJson(
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
