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

import sleeper.core.tracker.job.run.JobRunTime;

/**
 * A GSON plugin to write JSON for reports, on the run times of jobs held in a job tracker. This must be used in
 * combination with other GSON plugins for the specific type of jobs being processed.
 */
public class JsonJobRunTime {

    private JsonJobRunTime() {
    }

    /**
     * Creates a GSON serialiser for job run times. Handles {@link JobRunTime} objects.
     *
     * @return the GSON serialiser
     */
    public static JsonSerializer<JobRunTime> serializer() {
        return (runTime, type, context) -> createRunTimeJson(runTime, context);
    }

    private static JsonElement createRunTimeJson(JobRunTime runTime, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("startTime", context.serialize(runTime.getStartTime()));
        jsonObject.add("finishTime", context.serialize(runTime.getFinishTime()));
        jsonObject.addProperty("durationInSeconds", runTime.getDurationInSeconds());
        jsonObject.addProperty("timeInProcessInSeconds", runTime.getTimeInProcessInSeconds());
        return jsonObject;
    }

}
