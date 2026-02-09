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

package sleeper.clients.report.compaction.task;

import com.google.gson.Gson;

import sleeper.clients.util.ClientsGsonConfig;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

import java.io.PrintStream;
import java.util.List;

/**
 * Creates reports in JSON format on the status of compaction tasks.
 */
public class JsonCompactionTaskStatusReporter implements CompactionTaskStatusReporter {
    private final Gson gson = ClientsGsonConfig.standardBuilder().create();
    private final PrintStream out;

    public JsonCompactionTaskStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(CompactionTaskQuery query, List<CompactionTaskStatus> tasks) {
        out.println(gson.toJson(tasks));
    }

}
