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

package sleeper.clients.status.report.ingest.job;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.clients.status.report.job.JsonRecordsProcessedSummary;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.GsonConfig;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStatusUpdate;
import sleeper.ingest.job.status.IngestJobAcceptedStatus;
import sleeper.ingest.job.status.IngestJobRejectedStatus;
import sleeper.ingest.job.status.IngestJobStartedStatus;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobValidatedStatus;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

public class JsonIngestJobStatusReporter implements IngestJobStatusReporter {
    private final Gson gson = GsonConfig.standardBuilder()
            .registerTypeAdapter(RecordsProcessedSummary.class, JsonRecordsProcessedSummary.serializer())
            .registerTypeAdapter(IngestJobStatus.class, ingestJobStatusJsonSerializer())
            .registerTypeAdapter(ProcessRun.class, processRunJsonSerializer())
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
    public void report(List<IngestJobStatus> statusList, JobQuery.Type queryType, IngestQueueMessages queueMessages,
                       Map<String, Integer> persistentEmrStepCount) {
        out.println(gson.toJson(createJsonReport(statusList, queueMessages, persistentEmrStepCount)));
    }

    private JsonObject createJsonReport(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages,
                                        Map<String, Integer> persistentEmrStepCount) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("queueMessages", gson.toJsonTree(queueMessages));
        if (!persistentEmrStepCount.isEmpty()) {
            jsonObject.addProperty("pendingEMRSteps", persistentEmrStepCount.getOrDefault("PENDING", 0));
        }
        jsonObject.add("jobList", gson.toJsonTree(statusList));
        return jsonObject;
    }

    private static JsonSerializer<IngestJobStatus> ingestJobStatusJsonSerializer() {
        return (jobStatus, type, context) -> createIngestJobJson(jobStatus, context);
    }

    private static JsonElement createIngestJobJson(IngestJobStatus jobStatus, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("jobId", jobStatus.getJobId());
        jsonObject.add("jobRunList", context.serialize(jobStatus.getJobRuns()));
        return jsonObject;
    }

    private static JsonSerializer<IngestJobStartedStatus> ingestJobStartedStatusJsonSerializer() {
        return (jobStatus, type, context) -> createStartedStatusJson(jobStatus);
    }

    private static JsonElement createStartedStatusJson(IngestJobStartedStatus startedUpdate) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("inputFileCount", startedUpdate.getInputFileCount());
        jsonObject.addProperty("startTime", startedUpdate.getStartTime().toString());
        jsonObject.addProperty("updateTime", startedUpdate.getUpdateTime().toString());
        return jsonObject;
    }

    public static JsonSerializer<ProcessRun> processRunJsonSerializer() {
        return ((processRun, type, context) -> createProcessRunJson(processRun, context));
    }

    private static JsonElement createProcessRunJson(ProcessRun run, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("taskId", run.getTaskId());
        for (ProcessStatusUpdate update : run.getStatusUpdates()) {
            if (update instanceof IngestJobValidatedStatus) {
                jsonObject.add("validatedStatus",
                        processValidatedStatus((IngestJobValidatedStatus) update, context));
            } else if (update instanceof IngestJobStartedStatus) {
                jsonObject.add("startedStatus", context.serialize(update));
            } else if (update instanceof ProcessFinishedStatus) {
                jsonObject.add("finishedStatus", context.serialize(update));
            }
        }
        return jsonObject;
    }

    private static JsonElement processValidatedStatus(IngestJobValidatedStatus update, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("isValid", update instanceof IngestJobAcceptedStatus);
        jsonObject.add("inputFileCount", context.serialize(update.getInputFileCount()));
        jsonObject.add("validatedTime", context.serialize(update.getStartTime()));
        jsonObject.add("updateTime", context.serialize(update.getUpdateTime()));
        if (update instanceof IngestJobRejectedStatus) {
            IngestJobRejectedStatus rejectedStatus = (IngestJobRejectedStatus) update;
            jsonObject.add("reasons", context.serialize(rejectedStatus.getReasons()));
        }
        return jsonObject;
    }
}
