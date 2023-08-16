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

package sleeper.systemtest.suite.dsl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.systemtest.drivers.ingest.IngestReportsDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.drivers.util.TestContext;

import java.time.Instant;

public class SystemTestIngestReporting {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTestIngestReporting.class);
    private static Instant recordingFromTime = Instant.now();
    private final IngestReportsDriver driver;

    public SystemTestIngestReporting(SleeperInstanceContext instance, SystemTestClients clients, SystemTestParameters parameters) {
        this.driver = new IngestReportsDriver(clients.getDynamoDB(), clients.getSqs(), clients.getEmr(), instance, parameters);
    }

    public void startRecording() {
        recordingFromTime = Instant.now();
        LOGGER.info("Ingest recording window started at {}", recordingFromTime);
    }

    public void printTasksAndJobs(TestContext testContext) {
        driver.printReports(testContext, recordingFromTime);
    }
}
