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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.tracker.job.run.JobRunSummary;

/**
 * A GSON plugin to write JSON for reports, on summaries of runs of jobs held in a job tracker. This must be used in
 * combination with other GSON plugins for the specific type of jobs being processed.
 */
public class JsonJobRunSummary {

    private JsonJobRunSummary() {
    }

    /**
     * Creates a GSON serialiser for job run summaries. Handles {@link JobRunSummary} objects.
     *
     * @return the GSON serialiser
     */
    public static JsonSerializer<JobRunSummary> serializer() {
        return (summary, type, context) -> createSummaryJson(summary, context);
    }

    private static JsonElement createSummaryJson(JobRunSummary summary, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("rowsProcessed", context.serialize(summary.getRowsProcessed()));
        jsonObject.add("runTime", context.serialize(summary.getRunTime()));
        jsonObject.addProperty("rowsReadPerSecond", summary.getRowsReadPerSecond());
        jsonObject.addProperty("rowsWrittenPerSecond", summary.getRowsWrittenPerSecond());
        return jsonObject;
    }
}
