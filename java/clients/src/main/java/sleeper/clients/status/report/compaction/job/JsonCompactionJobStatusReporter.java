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

package sleeper.clients.status.report.compaction.job;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.clients.status.report.job.JsonRecordsProcessedSummary;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.GsonConfig;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.io.PrintStream;
import java.util.List;

public class JsonCompactionJobStatusReporter implements CompactionJobStatusReporter {
    private final Gson gson = GsonConfig.standardBuilder()
            .registerTypeAdapter(RecordsProcessedSummary.class, JsonRecordsProcessedSummary.serializer())
            .registerTypeAdapter(CompactionJobStatus.class, compactionJobStatusJsonSerializer())
            .create();
    private final PrintStream out;

    public JsonCompactionJobStatusReporter() {
        this(System.out);
    }

    public JsonCompactionJobStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(List<CompactionJobStatus> statusList, JobQuery.Type queryType) {
        out.println(gson.toJson(statusList));
    }

    private static JsonSerializer<CompactionJobStatus> compactionJobStatusJsonSerializer() {
        return (jobStatus, type, context) -> createCompactionJobJson(jobStatus, context);
    }

    private static JsonElement createCompactionJobJson(CompactionJobStatus jobStatus, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("jobId", jobStatus.getJobId());
        jsonObject.add("jobRunList", context.serialize(jobStatus.getJobRuns()));
        JsonObject createdStatus = new JsonObject();
        createdStatus.addProperty("updateTime", jobStatus.getCreateUpdateTime().toString());
        createdStatus.addProperty("partitionId", jobStatus.getPartitionId());
        createdStatus.add("childPartitionIds", context.serialize(jobStatus.getChildPartitionIds()));
        createdStatus.addProperty("inputFilesCount", jobStatus.getInputFilesCount());
        jsonObject.add("createdStatus", createdStatus);
        return jsonObject;
    }
}
