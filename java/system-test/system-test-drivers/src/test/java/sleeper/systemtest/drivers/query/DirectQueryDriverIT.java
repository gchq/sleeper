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
package sleeper.systemtest.drivers.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperDsl;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;

@LocalStackDslTest
public class DirectQueryDriverIT {

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(LOCALSTACK_MAIN);
    }

    @Test
    void shouldQueryFile(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.of(1, 3, 2));

        // When / Then
        assertThat(sleeper.directQuery().allRowsInTable()).containsExactlyElementsOf(
                sleeper.generateNumberedRows().iterableOver(1, 2, 3));
    }

}
