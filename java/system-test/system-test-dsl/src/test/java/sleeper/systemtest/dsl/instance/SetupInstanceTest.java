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

package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class SetupInstanceTest {
    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstance(IN_MEMORY_MAIN);
    }

    @Test
    void shouldConnectToInstance(SleeperSystemTest sleeper) {
        assertThat(sleeper.instanceProperties().getBoolean(RETAIN_INFRA_AFTER_DESTROY))
                .isFalse();
    }

    @Test
    void shouldIngestOneRecord(SleeperSystemTest sleeper) {
        // Given
        Record record = new Record(Map.of(
                "key", "some-id",
                "timestamp", 1234L,
                "value", "Some value"));

        // When
        sleeper.ingest().direct(null).records(record);

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactly(record);
    }
}
