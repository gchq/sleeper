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

package sleeper.systemtest.dsl.statemachine;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * DSL entry point for querying the status of bulk import jobs in an AWS Step Functions state machine. Resolves the
 * state machine ARN and table ID from the current instance context so that tests only need to provide job IDs.
 */
public class StateMachineDsl {

    private final SystemTestInstanceContext instance;
    private final StateMachineDriver driver;

    public StateMachineDsl(SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.instance = context.instance();
        this.driver = baseDrivers.statemachine(context);
    }

    /**
     * Retrieves the execution status of each bulk import job in the EKS state machine.
     *
     * @param  jobIds the bulk import job IDs to query
     * @return        a map of job ID to execution status (e.g. "SUCCEEDED", "FAILED", "RUNNING")
     */
    public Map<String, String> getJobExecutionStatuses(List<String> jobIds) {
        String stateMachineArn = instance.getInstanceProperties().get(BULK_IMPORT_EKS_STATE_MACHINE_ARN);
        String tableId = instance.getTableProperties().get(TABLE_ID);
        return driver.getJobExecutionStatuses(stateMachineArn, tableId, jobIds);
    }

    /**
     * Polls until all state machine executions have finished, then returns the final statuses.
     *
     * @param  jobIds the bulk import job IDs to query
     * @param  poll   the polling configuration
     * @return        a map of job ID to final execution status
     */
    public Map<String, String> waitForJobsFinished(List<String> jobIds, PollWithRetries poll) {
        try {
            return poll.queryUntil("state machine executions finished",
                    () -> getJobExecutionStatuses(jobIds),
                    statuses -> statuses.values().stream().noneMatch("RUNNING"::equals));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
