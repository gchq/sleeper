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

package sleeper.status.report.ingest.task;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import sleeper.ingest.task.IngestTaskStatus;

import java.io.PrintStream;
import java.time.Instant;
import java.util.List;

public class JsonIngestTaskStatusReporter implements IngestTaskStatusReporter {
    private final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues()
            .registerTypeAdapter(Instant.class, instantJsonSerializer())
            .setPrettyPrinting()
            .create();
    private final PrintStream out;

    public JsonIngestTaskStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(IngestTaskQuery query, List<IngestTaskStatus> tasks) {
        out.println(gson.toJson(tasks));
    }

    private static JsonSerializer<Instant> instantJsonSerializer() {
        return (instant, type, context) -> new JsonPrimitive(instant.toString());
    }
}
