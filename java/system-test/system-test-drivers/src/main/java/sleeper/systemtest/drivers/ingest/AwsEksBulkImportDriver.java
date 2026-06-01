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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsEksBulkImportDriver.class);

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
                    LOGGER.info("Found execution for job {}: {}", jobId, response);
                    if (response.error() != null) {
                        LOGGER.info("Error: {}", response.error());
                        LOGGER.info("Cause: {}", response.cause());
                    }
                    return response.statusAsString();
                }).toList();
    }

    @Override
    public List<String> getPods() {
        InstanceProperties properties = instance.getInstanceProperties();
        PodList list = k8sProvider.getClient(properties).pods()
                .inNamespace(properties.get(BULK_IMPORT_EKS_NAMESPACE))
                .list();
        LOGGER.info("Found pods in Spark namespace: {}", list);
        return list.getItems().stream().map(Pod::toString).toList();
    }

    @Override
    public List<String> getJobs() {
        InstanceProperties properties = instance.getInstanceProperties();
        JobList list = k8sProvider.getClient(properties).batch().v1().jobs()
                .inNamespace(properties.get(BULK_IMPORT_EKS_NAMESPACE))
                .list();
        LOGGER.info("Found jobs in Spark namespace: {}", list);
        return list.getItems().stream().map(Job::toString).toList();
    }

    public interface KubernetesClientProvider {
        KubernetesClient getClient(InstanceProperties instanceProperties);
    }
}
