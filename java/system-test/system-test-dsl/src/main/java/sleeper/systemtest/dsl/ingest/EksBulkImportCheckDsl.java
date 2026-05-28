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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.util.Collection;

/**
 * DSL entry point for querying the status of bulk import jobs in an AWS Step Functions state machine.
 */
public class EksBulkImportCheckDsl {

    private final EksBulkImportDriver driver;
    private final PollWithRetriesDriver pollDriver;

    public EksBulkImportCheckDsl(SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.driver = baseDrivers.eksBulkImport(context);
        this.pollDriver = baseDrivers.pollWithRetries();
    }

    /**
     * Polls until all state machine executions have finished, then returns the final statuses.
     *
     * @param  poll the polling configuration
     * @return      a map of job ID to final execution status
     */
    public Collection<String> waitUntilExecutionsFinishedGetStatuses(PollWithRetries poll) {
        try {
            return pollDriver.poll(poll)
                    .queryUntil("state machine executions finished",
                            () -> driver.getExecutionStatuses(),
                            statuses -> statuses.stream().noneMatch("RUNNING"::equals));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
