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

package sleeper.clients.status.report.ingest.job;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.clients.status.report.job.JsonProcessRunTime;
import sleeper.clients.status.report.job.JsonRecordsProcessedSummary;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.ClientsGsonConfig;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobUpdateType;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.core.tracker.job.ProcessRunTime;
import sleeper.core.tracker.job.RecordsProcessedSummary;
import sleeper.core.tracker.job.status.ProcessRuns;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static sleeper.clients.status.report.job.JsonProcessRunReporter.processRunsJsonSerializer;

public class JsonIngestJobStatusReporter implements IngestJobStatusReporter {
    private final Gson gson = ClientsGsonConfig.standardBuilder()
            .registerTypeAdapter(RecordsProcessedSummary.class, JsonRecordsProcessedSummary.serializer())
            .registerTypeAdapter(ProcessRunTime.class, JsonProcessRunTime.serializer())
            .registerTypeAdapter(ProcessRuns.class, processRunsJsonSerializer(IngestJobUpdateType::typeOfUpdate))
            .registerTypeAdapter(IngestJobStartedStatus.class, ingestJobStartedStatusJsonSerializer())
            .create();
    private final PrintStream out;

    public JsonIngestJobStatusReporter() {
        this(System.out);
    }

    public JsonIngestJobStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(
            List<IngestJobStatus> statusList, JobQuery.Type queryType, IngestQueueMessages queueMessages,
            Map<String, Integer> persistentEmrStepCount) {
        out.println(gson.toJson(createJsonReport(statusList, queueMessages, persistentEmrStepCount)));
    }

    private JsonObject createJsonReport(
            List<IngestJobStatus> statusList, IngestQueueMessages queueMessages,
            Map<String, Integer> persistentEmrStepCount) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("queueMessages", gson.toJsonTree(queueMessages));
        if (!persistentEmrStepCount.isEmpty()) {
            jsonObject.addProperty("pendingEMRSteps", persistentEmrStepCount.getOrDefault("PENDING", 0));
        }
        jsonObject.add("jobList", gson.toJsonTree(statusList));
        return jsonObject;
    }

    private static JsonSerializer<IngestJobStartedStatus> ingestJobStartedStatusJsonSerializer() {
        return (jobStatus, type, context) -> createStartedStatusJson(jobStatus, context);
    }

    private static JsonElement createStartedStatusJson(
            IngestJobStartedStatus startedUpdate, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("inputFileCount", startedUpdate.getInputFileCount());
        jsonObject.add("startTime", context.serialize(startedUpdate.getStartTime()));
        jsonObject.add("updateTime", context.serialize(startedUpdate.getUpdateTime()));
        return jsonObject;
    }

}
