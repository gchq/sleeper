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

package sleeper.systemtest.drivers.ingest.json;

import com.google.gson.Gson;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awssdk.services.ecs.model.Task;

import sleeper.clients.util.ClientsGsonConfig;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class TasksSummaryJson {

    private static final Gson GSON = ClientsGsonConfig.standardBuilder().create();

    private final int numTasks;
    private final Map<String, Long> countByDesiredStatus = new TreeMap<>();
    private final Map<String, Long> countByLastStatus = new TreeMap<>();

    public TasksSummaryJson(List<Task> tasks) {
        numTasks = tasks.size();
        for (Task task : tasks) {
            countByDesiredStatus.compute(task.desiredStatus(),
                    (key, count) -> count == null ? 1 : count + 1);
            countByLastStatus.compute(task.lastStatus(),
                    (key, count) -> count == null ? 1 : count + 1);
        }
    }

    public String toString() {
        return GSON.toJson(this);
    }
}
