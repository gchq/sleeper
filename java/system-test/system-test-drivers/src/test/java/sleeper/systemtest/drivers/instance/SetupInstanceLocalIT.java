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
package sleeper.systemtest.drivers.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.row.Row;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperSystemTest;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;

@LocalStackDslTest
public class SetupInstanceLocalIT {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(LOCALSTACK_MAIN);
    }

    @Test
    void shouldConnectToInstance(SleeperSystemTest sleeper) {
        assertThat(sleeper.instanceProperties().getBoolean(RETAIN_INFRA_AFTER_DESTROY))
                .isFalse();
    }

    @Test
    void shouldIngestOneRow(SleeperSystemTest sleeper) {
        // Given
        Row row = new Row(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));

        // When
        sleeper.ingest().direct(tempDir).rows(row);

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactly(row);
    }
}
