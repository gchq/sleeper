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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BULK_IMPORT_PERFORMANCE;
import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

@Tag("SystemTest")
public class EmrBulkImportPerformanceIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(BULK_IMPORT_PERFORMANCE);
        sleeper.reporting().startRecording();
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        sleeper.reporting().printIngestJobs(testContext(testInfo));
    }
}
