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

package sleeper.status.report.compactionjob;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.io.PrintStream;
import java.util.List;

public class JsonCompactionJobStatusReporter implements CompactionJobStatusReporter {
    private final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues()
            .setPrettyPrinting()
            .setExclusionStrategies(new JsonCompactionJobStatusExcludes())
            .create();
    private final PrintStream out;

    public JsonCompactionJobStatusReporter() {
        this(System.out);
    }

    public JsonCompactionJobStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(List<CompactionJobStatus> statusList, QueryType queryType) {
        out.println(gson.toJson(statusList));
    }
}
