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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.record.process.status.ProcessRun;

public class JsonProcessRunReporter {
    private JsonProcessRunReporter() {
    }

    public static JsonSerializer<ProcessRun> processRunJsonSerializer() {
        return ((processRun, type, context) -> createProcessRunJson(processRun, context));
    }

    private static JsonElement createProcessRunJson(ProcessRun run, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("taskId", run.getTaskId());
        jsonObject.add("startedStatus", context.serialize(run.getStartedStatus()));
        if (run.isFinished()) {
            jsonObject.add("finishedStatus", context.serialize(run.getFinishedStatus()));
        }
        return jsonObject;
    }
}
