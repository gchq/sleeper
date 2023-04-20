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
package sleeper.systemtest.ingest.json;

import com.google.gson.Gson;

import sleeper.clients.util.GsonConfig;
import sleeper.ingest.task.IngestTaskStatus;

import java.time.Instant;

public class IngestTaskStatusJson {

    private static final Gson GSON = GsonConfig.standardBuilder().create();

    private final String taskId;
    private final Instant startTime;

    public IngestTaskStatusJson(IngestTaskStatus status) {
        taskId = status.getTaskId();
        startTime = status.getStartTime();
    }

    public String toString() {
        return GSON.toJson(this);
    }

    // These getters are just to convince Spotbugs these fields are used
    public String getTaskId() {
        return taskId;
    }

    public Instant getStartTime() {
        return startTime;
    }
}
