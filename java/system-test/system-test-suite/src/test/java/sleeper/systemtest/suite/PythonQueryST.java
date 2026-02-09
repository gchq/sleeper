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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.util.command.CommandFailedException;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class PythonQueryST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setup(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @AfterEach
    void tearDown(SleeperDsl sleeper) {
        sleeper.query().emptyResultsBucket();
    }

    @Test
    void shouldRunExactKeyQuery(SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(sleeper.pythonApi()
                .query(tempDir).exactKeys("key",
                        "row-0000000000000000001",
                        "row-0000000000000000002")
                .results())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRangeClosed(1, 2));
    }

    @Test
    void shouldRunRangeKeyQuery(SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(sleeper.pythonApi()
                .query(tempDir).range("key",
                        "row-0000000000000000010",
                        "row-0000000000000000020")
                .results())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(10, 20));
    }

    @Test
    void shouldRunRangeKeyQueryWithMinAndMaxInclusive(SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When/Then
        assertThat(sleeper.pythonApi()
                .query(tempDir).range("key",
                        "row-0000000000000000010", true,
                        "row-0000000000000000020", true)
                .results())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRangeClosed(10, 20));
    }

    @Test
    void shouldFailToRunRangeKeyQueryWithNonExistentTable(SleeperDsl sleeper) {
        // When/Then
        assertThatThrownBy(() -> sleeper.pythonApi()
                .query(tempDir).range("key", "not-a-table",
                        "row-0000000000000000010",
                        "row-0000000000000000020")
                .results())
                .isInstanceOf(CommandFailedException.class);
    }

    @Test
    void shouldFailToRunRangeKeyQueryWithNonExistentKey(SleeperDsl sleeper) {
        // When/Then
        assertThatThrownBy(() -> sleeper.pythonApi()
                .query(tempDir).range("not-a-key",
                        "row-0000000000000000010",
                        "row-0000000000000000020")
                .results())
                .isInstanceOf(CommandFailedException.class);
    }
}
