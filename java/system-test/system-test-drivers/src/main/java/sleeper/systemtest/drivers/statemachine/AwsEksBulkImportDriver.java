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

import sleeper.bulkimport.core.statemachine.DeriveJobExecutionName;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.EksBulkImportDriver;
import sleeper.systemtest.dsl.ingest.SentIngestJobsContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.LinkedHashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Queries AWS Step Functions for the execution status of bulk import jobs. Derives execution names from job IDs and
 * table IDs using DeriveJobExecutionName, then calls the Step Functions API to retrieve each execution's status.
 */
public class AwsEksBulkImportDriver implements EksBulkImportDriver {
    private final SystemTestInstanceContext instance;
    private final SentIngestJobsContext sentJobs;
    private final SfnClient sfnClient;

    public AwsEksBulkImportDriver(SystemTestInstanceContext instance, SentIngestJobsContext sentJobs, SystemTestClients clients) {
        this.instance = instance;
        this.sentJobs = sentJobs;
        this.sfnClient = clients.getSfn();
    }

    @Override
    public Map<String, String> getExecutionStatuses() {
        Map<String, String> statuses = new LinkedHashMap<>();
        String stateMachineArn = instance.getInstanceProperties().get(BULK_IMPORT_EKS_STATE_MACHINE_ARN);
        String tableId = instance.getTableProperties().get(TABLE_ID);
        for (String jobId : sentJobs.getJobIds()) {
            String executionName = DeriveJobExecutionName.jobExecutionName(tableId, jobId);
            String executionArn = stateMachineArn.replace(":stateMachine:", ":execution:") + ":" + executionName;
            String status = sfnClient.describeExecution(req -> req.executionArn(executionArn)).statusAsString();
            statuses.put(jobId, status);
        }
        return statuses;
    }
}
