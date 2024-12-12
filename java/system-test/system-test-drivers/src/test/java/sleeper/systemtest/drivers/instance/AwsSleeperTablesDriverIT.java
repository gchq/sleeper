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

package sleeper.systemtest.drivers.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperSystemTest;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@LocalStackDslTest
public class AwsSleeperTablesDriverIT {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(LOCALSTACK_MAIN);
    }

    @Test
    void shouldAddOneTable(SleeperSystemTest sleeper) {
        sleeper.tables().create("A", DEFAULT_SCHEMA);
        assertThat(sleeper.tables().list())
                .containsExactly(sleeper.table("A").tableProperties().getStatus());
    }

    @Test
    void shouldInitialiseTablePartitions(SleeperSystemTest sleeper) {
        sleeper.tables().create("A", DEFAULT_SCHEMA);
        assertThat(sleeper.table("A").partitioning().tree().getAllPartitions())
                .hasSize(1);
    }
}
