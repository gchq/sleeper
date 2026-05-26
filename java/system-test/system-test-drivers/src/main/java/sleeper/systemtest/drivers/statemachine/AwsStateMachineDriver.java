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

package sleeper.systemtest.drivers.statemachine;

import software.amazon.awssdk.services.sfn.SfnClient;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.statemachine.DeriveJobExecutionName;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.EksBulkImportDriver;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Queries AWS Step Functions for the execution status of bulk import jobs. Derives execution names from job IDs and
 * table IDs using DeriveJobExecutionName, then calls the Step Functions API to retrieve each execution's status.
 */
public class AwsStateMachineDriver implements EksBulkImportDriver {
    private final SfnClient sfnClient;

    public AwsStateMachineDriver(SystemTestClients clients) {
        this.sfnClient = clients.getSfn();
    }

    @Override
    public Map<String, String> getJobExecutionStatuses(String stateMachineArn, String tableId, List<String> jobIds) {
        Map<String, String> statuses = new LinkedHashMap<>();
        for (String jobId : jobIds) {
            String executionName = DeriveJobExecutionName.jobExecutionName(
                    BulkImportJob.builder().id(jobId).tableId(tableId).build());
            String executionArn = stateMachineArn.replace(":stateMachine:", ":execution:") + ":" + executionName;
            String status = sfnClient.describeExecution(req -> req.executionArn(executionArn)).statusAsString();
            statuses.put(jobId, status);
        }
        return statuses;
    }
}
