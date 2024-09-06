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
package sleeper.systemtest.dsl.statestore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.InMemorySystemTestDrivers;
import sleeper.systemtest.dsl.testutil.drivers.InMemoryStateStoreCommitter;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.MAIN;

@InMemoryDslTest
public class SystemTestStateStoreFakeCommitsThroughputTest {

    private InMemoryStateStoreCommitter committer;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, InMemorySystemTestDrivers drivers) {
        committer = drivers.stateStoreCommitter();
    }

    @Test
    void shouldAssertOnCommitsPerSecond(SleeperSystemTest sleeper) {
        // Given
        sleeper.connectToInstance(MAIN);
        committer.setFakeCommitsPerSecond(sleeper, 10.0);

        // When / Then
        assertThat(sleeper.stateStore().commitsPerSecondForTable())
                .isEqualTo(10.0);
    }

    @Test
    void shouldAssertOnCommitsPerSecondForMultipleTables(SleeperSystemTest sleeper) {
        // Given
        sleeper.connectToInstanceNoTables(MAIN);
        sleeper.tables().create(List.of("A", "B"), DEFAULT_SCHEMA);
        committer.setFakeCommitsPerSecond(sleeper.table("A"), 10.0);
        committer.setFakeCommitsPerSecond(sleeper.table("B"), 20.0);

        // When / Then
        assertThat(sleeper.stateStore().commitsPerSecondByTable())
                .isEqualTo(Map.of("A", 10.0, "B", 20.0));
    }

}
