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
package sleeper.bulkimport.starter.executor;

import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrserverless.model.JobDriver;
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CLASS_NAME;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A {@link PlatformExecutor} which runs a bulk import job on EMR Serverless.
 */
public class EmrServerlessPlatformExecutor implements PlatformExecutor {
    private final EmrServerlessClient emrClient;
    private final InstanceProperties instanceProperties;

    public EmrServerlessPlatformExecutor(EmrServerlessClient emrClient,
                                         InstanceProperties instanceProperties) {
        this.emrClient = emrClient;
        this.instanceProperties = instanceProperties;
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET); // Todo change to own
        String clusterName = instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME);
        String jobName = String.join("-", "job", arguments.getJobRunId());
        String logUri = bulkImportBucket.isEmpty() ? "s3://" + clusterName
                : "s3://" + bulkImportBucket;

        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        String taskId = String.join("-", "sleeper", instanceProperties.get(ID),
                bulkImportJob.getTableName(), bulkImportJob.getId());
        if (taskId.length() > 64) {
            taskId = taskId.substring(0, 64);
        }

        StartJobRunRequest job = StartJobRunRequest.builder()
                .applicationId(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID))
                .name(jobName)
                .executionRoleArn(
                        instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN))
                .jobDriver(JobDriver.builder()
                        .sparkSubmit(SparkSubmit.builder()
                                .entryPoint("/workdir/bulk-import-runner.jar")
                                .entryPointArguments(
                                        instanceProperties.get(CONFIG_BUCKET),
                                        bulkImportJob.getId(), taskId, arguments.getJobRunId())
                                .sparkSubmitParameters(constructSparkArgs(instanceProperties))
                                .build())
                        .build())
                .configurationOverrides(
                        ConfigurationOverrides.builder()
                                .monitoringConfiguration(MonitoringConfiguration.builder()
                                        .s3MonitoringConfiguration(S3MonitoringConfiguration
                                                .builder().logUri(logUri).build())
                                        .build())
                                .build())
                .build();
        emrClient.startJobRun(job);
    }

    private String constructSparkArgs(InstanceProperties instanceProperties) {
        String javaHome = instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME);
        return "--class " + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLASS_NAME)
                + " --conf spark.executorEnv.JAVA_HOME=" + javaHome
                + " --conf spark.emr-serverless.driverEnv.JAVA_HOME=" + javaHome
                + " --conf spark.executor.cores="
                + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES)
                + " --conf spark.executor.memory="
                + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY)
                + " --conf spark.executor.instances="
                + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES)
                + " --conf spark.driver.cores="
                + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES)
                + " --conf spark.driver.memory="
                + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY);
    }
}
