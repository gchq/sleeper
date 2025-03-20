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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.FileReference;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.List;

import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.FileReferenceSystemTestHelper.fileFactory;

@SystemTest
public class CompactionDispatchFailureST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceAddOfflineTable(MAIN);
    }

    @Test
    void shouldSendBadCompactionBatchToDeadLetterQueue(SleeperSystemTest sleeper) throws Exception {
        // Given
        FileReference inputFile = fileFactory(sleeper).rootFile("input.parquet", 100);
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            update(store).addFile(inputFile);
            update(store).assignJobId("job-1", List.of(inputFile));
        });

        // When
        sleeper.compaction()
                .sendFakeSingleCompactionBatch("job-2", List.of(inputFile))
                // Then
                .waitForCompactionBatchOnDeadLetterQueue();
    }

}
