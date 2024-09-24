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
package sleeper.systemtest.drivers.ingest;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.Task;

import sleeper.systemtest.drivers.ingest.json.TasksJson;

import java.io.StringReader;
import java.time.Instant;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class TasksJsonTest {

    @Test
    public void shouldOutputJson() {
        // Given
        List<RunTaskResponse> responses = List.of(RunTaskResponse.builder()
                .tasks(Task.builder()
                        .taskArn("some-task").clusterArn("some-cluster")
                        .createdAt(Instant.parse("2022-12-22T10:08:01Z"))
                        .build())
                .build());

        // When
        String json = TasksJson.from(responses);

        // Then
        assertThatJson(json).isEqualTo("{\"tasks\":[" +
                "{\"taskArn\":\"some-task\",\"clusterArn\":\"some-cluster\",\"createdAt\":\"2022-12-22T10:08:01Z\"}]}");
    }

    @Test
    public void shouldReadJson() {
        List<Task> results = TasksJson.readTasks(new StringReader("{\"tasks\":[" +
                "{\"taskArn\":\"some-task\",\"clusterArn\":\"some-cluster\",\"createdAt\":\"2022-12-22T10:08:01Z\"}]}"));

        assertThat(results)
                .containsExactly(Task.builder()
                        .taskArn("some-task").clusterArn("some-cluster")
                        .createdAt(Instant.parse("2022-12-22T10:08:01Z"))
                        .build());
    }
}
