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
import software.amazon.awssdk.services.ecs.model.Container;
import software.amazon.awssdk.services.ecs.model.Task;
import software.amazon.awssdk.services.ecs.model.TaskStopCode;

import sleeper.clients.util.ClientsGsonConfig;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class TaskStatusJson {

    private static final Gson GSON = ClientsGsonConfig.standardBuilder().create();

    private final String taskArn;
    private final String clusterArn;
    private final String desiredStatus;
    private final String lastStatus;
    private final Map<String, String> containersLastStatus;
    private final Instant createdAt;
    private final Instant startedAt;
    private final Instant stoppingAt;
    private final Instant stoppedAt;
    private final TaskStopCode stopCode;
    private final String stoppedReason;

    public TaskStatusJson(Task task) {
        taskArn = task.taskArn();
        clusterArn = task.clusterArn();
        desiredStatus = task.desiredStatus();
        lastStatus = task.lastStatus();
        containersLastStatus = task.containers().stream()
                .collect(Collectors.toMap(Container::name, Container::lastStatus));
        createdAt = task.createdAt();
        startedAt = task.startedAt();
        stoppingAt = task.stoppingAt();
        stoppedAt = task.stoppedAt();
        stopCode = task.stopCode();
        stoppedReason = task.stoppedReason();
    }

    public String toString() {
        return GSON.toJson(this);
    }

}
