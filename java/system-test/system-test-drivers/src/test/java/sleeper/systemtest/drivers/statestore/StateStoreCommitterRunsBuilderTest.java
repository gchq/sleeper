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
package sleeper.systemtest.drivers.statestore;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResultField;

import sleeper.systemtest.dsl.statestore.StateStoreCommitterRun;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterRunsBuilderTest {
    StateStoreCommitterRunsBuilder builder = new StateStoreCommitterRunsBuilder();

    @Test
    void shouldBuildSingleRunNoCommits() {
        add(
                "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda started at 2024-08-13T12:12:00Z",
                "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda finished at 2024-08-13T12:13:00Z (ran for 1 minute)");
        assertThat(builder.buildRuns()).containsExactly(
                new StateStoreCommitterRun(Instant.parse("2024-08-13T12:12:00Z"), Instant.parse("2024-08-13T12:13:00Z"), List.of()));
    }

    void add(String... messages) {
        Instant startTime = Instant.now();
        for (int i = 0; i < messages.length; i++) {
            add(startTime.plus(Duration.ofMillis(i)), "test-logstream", messages[i]);
        }
    }

    void add(Instant timestamp, String logStream, String message) {
        builder.add(List.of(
                ResultField.builder().field("@timestamp").value("" + timestamp.toEpochMilli()).build(),
                ResultField.builder().field("@logStream").value(logStream).build(),
                ResultField.builder().field("@message").value(message).build()));
    }

}
