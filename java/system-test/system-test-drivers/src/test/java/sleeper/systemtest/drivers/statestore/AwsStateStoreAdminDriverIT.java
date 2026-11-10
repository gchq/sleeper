/*
 * Copyright 2022-2026 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.systemtest.drivers.testutil.LocalAwsDslTest;
import sleeper.systemtest.drivers.testutil.LocalAwsSystemTestDrivers;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.SystemTestContext;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.systemtest.drivers.testutil.LocalAwsTestInstance.LOCAL_MAIN;

@LocalAwsDslTest
public class AwsStateStoreAdminDriverIT {

    DynamoDBTransactionLogSnapshotMetadataStore metadataStore;

    @BeforeEach
    void setUp(SleeperDsl sleeper, SystemTestContext context, LocalAwsSystemTestDrivers drivers) {
        sleeper.connectToInstanceAddOnlineTable(LOCAL_MAIN);
        metadataStore = new DynamoDBTransactionLogSnapshotMetadataStore(
                context.instance().getInstanceProperties(),
                context.instance().getTableProperties(),
                drivers.clients().getDynamo());
    }

    @Test
    void shouldCreateUnreadableSnapshot(SleeperDsl sleeper) {
        // When
        sleeper.stateStore().addUnreadableFilesSnapshot();

        // Then
        assertThatThrownBy(() -> sleeper.tableFiles().all())
                .isInstanceOf(StateStoreException.class);
        assertThatThrownBy(() -> metadataStore.getLatestSnapshotsBefore(Instant.now().plus(Duration.ofDays(1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
