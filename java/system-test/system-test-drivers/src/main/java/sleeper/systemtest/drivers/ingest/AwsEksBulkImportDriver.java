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

package sleeper.systemtest.drivers.ingest;

import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionResponse;

import sleeper.bulkimport.core.statemachine.DeriveJobExecutionName;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.EksBulkImportDriver;
import sleeper.systemtest.dsl.ingest.SentIngestJobsContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE;
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
    private final KubernetesClientProvider k8sProvider;

    public AwsEksBulkImportDriver(SystemTestInstanceContext instance, SentIngestJobsContext sentJobs, SystemTestClients clients) {
        this(instance, sentJobs, clients.getSfn(), clients::createKubernetesClient);
    }

    public AwsEksBulkImportDriver(SystemTestInstanceContext instance, SentIngestJobsContext sentJobs, SfnClient sfnClient, KubernetesClientProvider k8sProvider) {
        this.instance = instance;
        this.sentJobs = sentJobs;
        this.sfnClient = sfnClient;
        this.k8sProvider = k8sProvider;
    }

    @Override
    public List<String> getExecutionStatuses() {
        String stateMachineArn = instance.getInstanceProperties().get(BULK_IMPORT_EKS_STATE_MACHINE_ARN);
        String tableId = instance.getTableProperties().get(TABLE_ID);
        return sentJobs.getJobIds().stream()
                .map(jobId -> {
                    String executionName = DeriveJobExecutionName.jobExecutionName(tableId, jobId);
                    String executionArn = stateMachineArn.replace(":stateMachine:", ":execution:") + ":" + executionName;
                    DescribeExecutionResponse response = sfnClient.describeExecution(req -> req.executionArn(executionArn));
                    return response.statusAsString();
                }).toList();
    }

    @Override
    public List<String> getRunningPods() {
        InstanceProperties properties = instance.getInstanceProperties();
        PodList list = k8sProvider.getClient(properties).pods()
                .inNamespace(properties.get(BULK_IMPORT_EKS_NAMESPACE))
                .list();
        return list.getItems().stream().map(pod -> pod.toString()).toList();
    }

    public interface KubernetesClientProvider {
        KubernetesClient getClient(InstanceProperties instanceProperties);
    }
}
