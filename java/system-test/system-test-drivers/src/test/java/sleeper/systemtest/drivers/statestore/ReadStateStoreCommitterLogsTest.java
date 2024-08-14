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

import sleeper.systemtest.dsl.statestore.StateStoreCommitSummary;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterRunFinished;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterRunStarted;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadStateStoreCommitterLogsTest {

    @Test
    void shouldReadLambdaStarted() {
        // Given
        String message = "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda started at 2024-08-13T12:12:00Z";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", message))).isEqualTo(
                new StateStoreCommitterRunStarted("test-stream", Instant.parse("2024-08-13T12:12:00Z")));
    }

    @Test
    void shouldReadLambdaFinished() {
        // Given
        String message = "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda finished at 2024-08-13T12:13:00Z (ran for 1 minute)";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", message))).isEqualTo(
                new StateStoreCommitterRunFinished("test-stream", Instant.parse("2024-08-13T12:13:00Z")));
    }

    @Test
    void shouldReadCommitApplied() {
        // Given
        String message = "[main] sleeper.commit.StateStoreCommitter INFO - Applied request to table ID test-table with type TestRequest at time 2024-08-13T12:12:30Z";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", message))).isEqualTo(
                new StateStoreCommitSummary("test-stream", "test-table", "TestRequest", Instant.parse("2024-08-13T12:12:30Z")));
    }

    @Test
    void shouldReadUnrecognisedLog() {
        // Given
        String message = "some other log";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", message))).isNull();
    }

    @Test
    void shouldReadLogWithFieldsInReverseOrder() {
        // Given
        String message = "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda started at 2024-08-13T12:12:00Z";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntryReversed("test-stream", message))).isEqualTo(
                new StateStoreCommitterRunStarted("test-stream", Instant.parse("2024-08-13T12:12:00Z")));
    }

    @Test
    void shouldReadLogWithExtraFields() {
        // Given
        String message = "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda started at 2024-08-13T12:12:00Z";
        List<ResultField> entry = List.of(
                ResultField.builder().field("@extraField").value("some value").build(),
                ResultField.builder().field("@message").value(message).build(),
                ResultField.builder().field("@otherField").value("other value").build(),
                ResultField.builder().field("@logStream").value("test-stream").build(),
                ResultField.builder().field("@anotherField").value("another value").build());

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(entry))
                .isEqualTo(new StateStoreCommitterRunStarted("test-stream", Instant.parse("2024-08-13T12:12:00Z")));
    }

    private List<ResultField> logEntry(String logStream, String message) {
        return List.of(
                ResultField.builder().field("@logStream").value(logStream).build(),
                ResultField.builder().field("@message").value(message).build());
    }

    private List<ResultField> logEntryReversed(String logStream, String message) {
        return List.of(
                ResultField.builder().field("@message").value(message).build(),
                ResultField.builder().field("@logStream").value(logStream).build());
    }
}
