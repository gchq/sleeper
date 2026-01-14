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

package sleeper.clients.report.query;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.clients.util.ClientsGsonConfig;
import sleeper.query.core.tracker.TrackedQuery;

import java.io.PrintStream;
import java.time.Instant;
import java.util.List;

/**
 * Creates reports in JSON format on the status of queries.
 */
public class JsonQueryTrackerReporter implements QueryTrackerReporter {
    private final Gson gson = ClientsGsonConfig.standardBuilder()
            .registerTypeAdapter(TrackedQuery.class, trackedQueryJsonSerializer())
            .create();
    private final PrintStream out;

    public JsonQueryTrackerReporter() {
        this(System.out);
    }

    public JsonQueryTrackerReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(TrackerQuery query, List<TrackedQuery> queries) {
        out.println(gson.toJson(queries));
    }

    private static JsonSerializer<TrackedQuery> trackedQueryJsonSerializer() {
        return (jobStatus, type, context) -> createTrackedQueryJson(jobStatus, context);
    }

    private static JsonElement createTrackedQueryJson(TrackedQuery query, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("queryId", query.getQueryId());
        jsonObject.addProperty("subQueryId", query.getSubQueryId());
        jsonObject.add("lastUpdateTime", context.serialize(Instant.ofEpochMilli(query.getLastUpdateTime())));
        jsonObject.addProperty("lastKnownState", query.getLastKnownState().toString());
        jsonObject.addProperty("rowCount", query.getRowCount());
        jsonObject.addProperty("errorMessage", query.getErrorMessage());
        return jsonObject;
    }
}
