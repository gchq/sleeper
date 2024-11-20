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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.StateStoreException;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.MAIN;

@LocalStackDslTest
public class AwsSleeperTablesDriverIT {

    private SleeperInstanceDriver instanceDriver;
    private SleeperTablesDriver driver;
    private InstanceProperties instanceProperties;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, SystemTestDrivers drivers, SystemTestParameters parameters) {
        driver = drivers.tables(parameters);
        instanceDriver = drivers.instance(parameters);
        sleeper.connectToInstanceNoTables(MAIN);
        instanceProperties = sleeper.instanceProperties();
    }

    @Test
    void shouldAddOneTable(SleeperSystemTest sleeper) {
        sleeper.tables().create("A", DEFAULT_SCHEMA);
        assertThat(sleeper.tables().list())
                .containsExactly(sleeper.table("A").tableProperties().getStatus());
    }

    @Test
    void shouldInitialiseTablePartitions(SleeperSystemTest sleeper) throws StateStoreException {
        sleeper.tables().create("A", DEFAULT_SCHEMA);
        assertThat(sleeper.table("A").partitioning().tree().getAllPartitions())
                .hasSize(1);
    }
}
