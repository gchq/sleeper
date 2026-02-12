/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.report.statestore;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResultField;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadStateStoreCommitterLogsTest {

    @Test
    void shouldReadLambdaStarted() {
        // Given
        String message = "[main] lambda.committer.StateStoreCommitterLambda INFO - State store committer process started at 2024-08-13T12:12:00Z\n";
        Instant timestamp = Instant.parse("2024-08-13T12:12:30Z");

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", timestamp, message))).isEqualTo(
                new StateStoreCommitterRunStarted("test-stream", timestamp, Instant.parse("2024-08-13T12:12:00Z")));
    }

    @Test
    void shouldReadLambdaFinished() {
        // Given
        String message = "[main] lambda.committer.StateStoreCommitterLambda INFO - State store committer process finished at 2024-08-13T12:13:00Z (ran for 1 minute)\n";
        Instant timestamp = Instant.parse("2024-08-13T12:13:30Z");

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", timestamp, message))).isEqualTo(
                new StateStoreCommitterRunFinished("test-stream", timestamp, Instant.parse("2024-08-13T12:13:00Z")));
    }

    @Test
    void shouldReadCommitApplied() {
        // Given
        String message = "[main] sleeper.commit.StateStoreCommitter INFO - Applied request to table ID test-table with type TestRequest at time 2024-08-13T12:12:30Z\n";
        Instant timestamp = Instant.parse("2024-08-13T12:12:55Z");

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", timestamp, message))).isEqualTo(
                new StateStoreCommitSummary("test-stream", timestamp, "test-table", "TestRequest", Instant.parse("2024-08-13T12:12:30Z")));
    }

    @Test
    void shouldReadUnrecognisedLog() {
        // Given
        String message = "some other log\n";
        Instant timestamp = Instant.parse("2024-08-13T12:12:00Z");

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(logEntry("test-stream", timestamp, message))).isNull();
    }

    @Test
    void shouldReadLogWithFieldsInReverseOrder() {
        // Given
        String message = "[main] lambda.committer.StateStoreCommitterLambda INFO - State store committer process started at 2024-08-13T12:12:00Z\n";
        Instant timestamp = Instant.parse("2024-08-13T12:12:30Z");
        List<ResultField> entry = List.of(
                messageField(message),
                timestampField(timestamp),
                logStreamField("test-stream"));

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(entry)).isEqualTo(
                new StateStoreCommitterRunStarted("test-stream", timestamp, Instant.parse("2024-08-13T12:12:00Z")));
    }

    @Test
    void shouldReadLogWithExtraFields() {
        // Given
        String message = "[main] lambda.committer.StateStoreCommitterLambda INFO - State store committer process started at 2024-08-13T12:12:00Z\n";
        Instant timestamp = Instant.parse("2024-08-13T12:12:30Z");
        List<ResultField> entry = List.of(
                ResultField.builder().field("@extraField").value("some value").build(),
                messageField(message),
                ResultField.builder().field("@otherField").value("other value").build(),
                timestampField(timestamp),
                ResultField.builder().field("@anotherField").value("another value").build(),
                logStreamField("test-stream"),
                ResultField.builder().field("@anotherField").value("another value").build());

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(entry))
                .isEqualTo(new StateStoreCommitterRunStarted("test-stream", timestamp, Instant.parse("2024-08-13T12:12:00Z")));
    }

    @Test
    void shouldReadTimestampFromString() {
        // Given
        String message = "[main] lambda.committer.StateStoreCommitterLambda INFO - State store committer process started at 2024-08-13T12:12:00Z\n";
        List<ResultField> entry = List.of(
                messageField(message),
                timestampField("2024-08-13 12:12:30.123"),
                logStreamField("test-stream"));

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.read(entry)).isEqualTo(
                new StateStoreCommitterRunStarted("test-stream",
                        Instant.parse("2024-08-13T12:12:30.123Z"),
                        Instant.parse("2024-08-13T12:12:00Z")));
    }

    private List<ResultField> logEntry(String logStream, Instant timestamp, String message) {
        return List.of(
                logStreamField(logStream),
                timestampField(timestamp),
                messageField(message));
    }

    private ResultField logStreamField(String logStream) {
        return ResultField.builder().field("@logStream").value(logStream).build();
    }

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = ReadStateStoreCommitterLogs.TIMESTAMP_FORMATTER.withZone(ZoneOffset.UTC);

    private ResultField timestampField(Instant timestamp) {
        return timestampField(TIMESTAMP_FORMATTER.format(timestamp));
    }

    private ResultField timestampField(String timestamp) {
        return ResultField.builder().field("@timestamp").value(timestamp).build();
    }

    private ResultField messageField(String message) {
        return ResultField.builder().field("@message").value(message).build();
    }
}
