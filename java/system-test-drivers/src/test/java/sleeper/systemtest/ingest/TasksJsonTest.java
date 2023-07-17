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
package sleeper.systemtest.ingest;

import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Task;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.drivers.ingest.json.TasksJson;

import java.io.StringReader;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class TasksJsonTest {

    @Test
    public void shouldOutputJson() {
        // Given
        List<RunTaskResult> results = Collections.singletonList(new RunTaskResult()
                .withTasks(new Task()
                        .withTaskArn("some-task").withClusterArn("some-cluster")
                        .withCreatedAt(Date.from(Instant.parse("2022-12-22T10:08:01Z")))));

        // When
        String json = TasksJson.from(results);

        // Then
        assertThatJson(json).isEqualTo("{\"tasks\":[" +
                "{\"taskArn\":\"some-task\",\"clusterArn\":\"some-cluster\",\"createdAt\":\"2022-12-22T10:08:01Z\"}]}");
    }

    @Test
    public void shouldReadJson() {
        List<Task> results = TasksJson.readTasks(new StringReader("{\"tasks\":[" +
                "{\"taskArn\":\"some-task\",\"clusterArn\":\"some-cluster\",\"createdAt\":\"2022-12-22T10:08:01Z\"}]}"));

        assertThat(results).usingRecursiveFieldByFieldElementComparator()
                .containsExactly(new Task()
                        .withTaskArn("some-task").withClusterArn("some-cluster")
                        .withCreatedAt(Date.from(Instant.parse("2022-12-22T10:08:01Z"))));
    }
}
