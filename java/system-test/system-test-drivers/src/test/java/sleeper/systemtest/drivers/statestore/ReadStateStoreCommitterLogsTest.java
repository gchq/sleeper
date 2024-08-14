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

import sleeper.systemtest.drivers.statestore.ReadStateStoreCommitterLogs.LambdaFinished;
import sleeper.systemtest.drivers.statestore.ReadStateStoreCommitterLogs.LambdaStarted;
import sleeper.systemtest.dsl.statestore.StateStoreCommitSummary;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadStateStoreCommitterLogsTest {

    @Test
    void shouldReadLambdaStarted() {
        // Given
        String message = "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda started at 2024-08-13T12:12:00Z";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.readEvent("test-stream", message)).isEqualTo(
                new LambdaStarted(Instant.parse("2024-08-13T12:12:00Z")));
    }

    @Test
    void shouldReadLambdaFinished() {
        // Given
        String message = "[main] committer.lambda.StateStoreCommitterLambda INFO - Lambda finished at 2024-08-13T12:13:00Z (ran for 1 minute)";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.readEvent("test-stream", message)).isEqualTo(
                new LambdaFinished(Instant.parse("2024-08-13T12:13:00Z")));
    }

    @Test
    void shouldReadCommitApplied() {
        // Given
        String message = "[main] sleeper.commit.StateStoreCommitter INFO - Applied request to table ID test-table with type TestRequest at time 2024-08-13T12:12:30Z";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.readEvent("test-stream", message)).isEqualTo(
                new StateStoreCommitSummary("test-stream", "test-table", "TestRequest", Instant.parse("2024-08-13T12:12:30Z")));
    }

    @Test
    void shouldReadUnrecognisedLog() {
        // Given
        String message = "some other log";

        // When / Then
        assertThat(ReadStateStoreCommitterLogs.readEvent("test-stream", message)).isNull();
    }
}
