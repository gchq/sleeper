/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.systemtest.drivers.ingest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.Task;

import sleeper.systemtest.drivers.ingest.json.TasksJson;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TaskJsonIT {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteToFileWhenDirectoryExists() throws Exception {
        // Given
        Path path = tempDir.resolve("tasks.json");

        // When
        TasksJson.writeToFile(List.of(response(task("some-task"))), path);

        // Then
        assertThat(TasksJson.readTasksFromFile(path))
                .containsExactly(task("some-task"));
    }

    @Test
    void shouldCreateDirectoryWhenWritingToFile() throws Exception {
        // Given
        Path path = tempDir.resolve("generated/writeDataOutput.json");

        // When
        TasksJson.writeToFile(List.of(response(task("some-task"))), path);

        // Then
        assertThat(TasksJson.readTasksFromFile(path))
                .containsExactly(task("some-task"));
    }

    @Test
    void shouldCreateNestedDirectoriesWhenWritingToFile() throws Exception {
        // Given
        Path path = tempDir.resolve("a/b/c/tasks.json");

        // When
        TasksJson.writeToFile(List.of(response(task("some-task"))), path);

        // Then
        assertThat(TasksJson.readTasksFromFile(path))
                .containsExactly(task("some-task"));
    }

    @Test
    void shouldOverwriteExistingFile() throws Exception {
        // Given
        Path path = tempDir.resolve("generated/writeDataOutput.json");
        TasksJson.writeToFile(List.of(response(task("old-task"))), path);

        // When
        TasksJson.writeToFile(List.of(response(task("new-task"))), path);

        // Then
        assertThat(TasksJson.readTasksFromFile(path))
                .containsExactly(task("new-task"));
    }

    private static RunTaskResponse response(Task... tasks) {
        return RunTaskResponse.builder().tasks(tasks).build();
    }

    private static Task task(String taskArn) {
        return Task.builder()
                .taskArn(taskArn).clusterArn("some-cluster")
                .createdAt(Instant.parse("2022-12-22T10:08:01Z"))
                .build();
    }
}
