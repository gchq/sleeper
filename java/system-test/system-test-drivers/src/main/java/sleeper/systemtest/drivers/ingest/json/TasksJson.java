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
package sleeper.systemtest.drivers.ingest.json;

import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Task;
import com.google.gson.Gson;
import com.google.gson.JsonSerializer;

import sleeper.clients.util.GsonConfig;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class TasksJson {

    private static final Gson GSON = GsonConfig.standardBuilder()
            .registerTypeAdapter(Date.class, dateSerializer())
            .create();

    private final List<Task> tasks;

    public TasksJson(List<Task> tasks) {
        this.tasks = tasks;
    }

    public String toString() {
        return GSON.toJson(tasks);
    }

    public static void writeToFile(List<RunTaskResult> results, Path path) throws IOException {
        Files.write(path, from(results).getBytes(StandardCharsets.UTF_8));
    }

    public static List<Task> readTasksFromFile(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return readTasks(reader);
        }
    }

    public static String from(List<RunTaskResult> results) {
        return fromTaskJson(results.stream()
                .flatMap(result -> result.getTasks().stream())
                .collect(Collectors.toList()));
    }

    public static List<Task> readTasks(Reader json) {
        return GSON.fromJson(json, TasksJson.class).getTasks();
    }

    private static String fromTaskJson(List<Task> tasks) {
        return GSON.toJson(new TasksJson(tasks));
    }

    private static JsonSerializer<Date> dateSerializer() {
        return (date, type, context) -> context.serialize(date.toInstant());
    }

    public List<Task> getTasks() {
        return tasks;
    }
}
