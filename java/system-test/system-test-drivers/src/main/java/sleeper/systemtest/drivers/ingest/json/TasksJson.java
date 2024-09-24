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
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.Task;

import sleeper.clients.util.ClientsGsonConfig;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableList;

public class TasksJson {

    private static final Gson GSON = ClientsGsonConfig.standardBuilder()
            .registerTypeAdapter(List.class, new ListAdapter())
            .create();

    private final List<Task> tasks;

    public TasksJson(List<Task> tasks) {
        this.tasks = tasks;
    }

    public String toString() {
        return GSON.toJson(tasks);
    }

    public static void writeToFile(List<RunTaskResponse> responses, Path path) throws IOException {
        Files.write(path, from(responses).getBytes(StandardCharsets.UTF_8));
    }

    public static List<Task> readTasksFromFile(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return readTasks(reader);
        }
    }

    public static String from(List<RunTaskResponse> responses) {
        return fromTaskJson(responses.stream()
                .flatMap(result -> result.tasks().stream())
                .collect(Collectors.toList()));
    }

    public static List<Task> readTasks(Reader json) {
        return GSON.fromJson(json, TasksJson.class)
                .getTasks().stream()
                .map(task -> task.toBuilder().build())
                .collect(toUnmodifiableList());
    }

    private static String fromTaskJson(List<Task> tasks) {
        return GSON.toJson(new TasksJson(tasks));
    }

    public List<Task> getTasks() {
        return tasks;
    }

    private static class ListAdapter implements JsonSerializer<List<?>>, JsonDeserializer<List<?>> {

        @Override
        public List<?> deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            JsonArray array = element.getAsJsonArray();
            if (array == null || array.isEmpty()) {
                return DefaultSdkAutoConstructList.getInstance();
            }
            Type itemType = itemType(type);
            return array.asList().stream()
                    .map(item -> context.deserialize(item, itemType))
                    .collect(toUnmodifiableList());
        }

        @Override
        public JsonElement serialize(List<?> list, Type type, JsonSerializationContext context) {
            if (list == null || list.isEmpty()) {
                return JsonNull.INSTANCE;
            }
            JsonArray array = new JsonArray(list.size());
            Type itemType = itemType(type);
            for (Object item : list) {
                array.add(context.serialize(item, itemType));
            }
            return array;
        }

        private static Type itemType(Type listType) {
            ParameterizedType paramType = (ParameterizedType) listType;
            return paramType.getActualTypeArguments()[0];
        }
    }
}
