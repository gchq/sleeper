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

package sleeper.clients.status.report.compaction.job;

import com.google.gson.Gson;

import sleeper.clients.status.report.job.JsonProcessRunTime;
import sleeper.clients.status.report.job.JsonRecordsProcessedSummary;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.ClientsGsonConfig;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobUpdateType;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRuns;

import java.io.PrintStream;
import java.util.List;

import static sleeper.clients.status.report.job.JsonProcessRunReporter.processRunsJsonSerializer;

public class JsonCompactionJobStatusReporter implements CompactionJobStatusReporter {
    private final Gson gson = ClientsGsonConfig.standardBuilder()
            .registerTypeAdapter(RecordsProcessedSummary.class, JsonRecordsProcessedSummary.serializer())
            .registerTypeAdapter(ProcessRunTime.class, JsonProcessRunTime.serializer())
            .registerTypeAdapter(ProcessRuns.class, processRunsJsonSerializer(CompactionJobUpdateType::typeOfUpdate))
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
}
