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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@Tag("SystemTest")
public class QueryIT {
    @TempDir
    private Path tempDir;
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setup() {
        sleeper.connectToInstance(MAIN);
        sleeper.queryResults().emptyBucket();
    }

    @Test
    void shouldRunSQSQuery() throws InterruptedException {
        // Given
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

        // When/Then
        assertThat(sleeper.query().byQueue()
                .run("key", "00010", "00020")
                .waitForQuery().results())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(10, 20)));
    }
}
