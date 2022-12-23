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
package sleeper.systemtest.ingest.json;

import com.amazonaws.services.ecs.model.Container;
import com.amazonaws.services.ecs.model.Task;
import com.google.gson.Gson;
import sleeper.util.GsonConfig;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskStatusJson {

    private static final Gson GSON = GsonConfig.standardBuilder().create();

    private final String taskArn;
    private final String clusterArn;
    private final String desiredStatus;
    private final String lastStatus;
    private final Map<String, String> containersLastStatus;
    private final Instant createdAt;
    private final Instant startedAt;
    private final Instant stoppingAt;
    private final Instant stoppedAt;
    private final String stopCode;
    private final String stoppedReason;

    public TaskStatusJson(Task task) {
        taskArn = task.getTaskArn();
        clusterArn = task.getClusterArn();
        desiredStatus = task.getDesiredStatus();
        lastStatus = task.getLastStatus();
        containersLastStatus = task.getContainers().stream()
                .collect(Collectors.toMap(Container::getName, Container::getLastStatus));
        createdAt = instantOrNull(task.getCreatedAt());
        startedAt = instantOrNull(task.getStartedAt());
        stoppingAt = instantOrNull(task.getStoppingAt());
        stoppedAt = instantOrNull(task.getStoppedAt());
        stopCode = task.getStopCode();
        stoppedReason = task.getStoppedReason();
    }

    private static Instant instantOrNull(Date date) {
        if (date == null) {
            return null;
        } else {
            return date.toInstant();
        }
    }

    public String toString() {
        return GSON.toJson(this);
    }

    // These getters are just to convince Spotbugs these fields are used
    public String getTaskArn() {
        return taskArn;
    }

    public String getClusterArn() {
        return clusterArn;
    }

    public String getDesiredStatus() {
        return desiredStatus;
    }

    public String getLastStatus() {
        return lastStatus;
    }

    public Map<String, String> getContainersLastStatus() {
        return containersLastStatus;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getStoppingAt() {
        return stoppingAt;
    }

    public Instant getStoppedAt() {
        return stoppedAt;
    }

    public String getStopCode() {
        return stopCode;
    }

    public String getStoppedReason() {
        return stoppedReason;
    }
}
