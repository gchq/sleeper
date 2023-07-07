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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrserverless.model.JobDriver;
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse;

import sleeper.configuration.properties.InstanceProperties;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY;

/**
 * A {@link PlatformExecutor} which runs a bulk import job on EMR Serverless.
 */
public class EmrServerlessPlatformExecutor implements PlatformExecutor {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(EmrServerlessPlatformExecutor.class);
    private final EmrServerlessClient emrClient;
    private final InstanceProperties instanceProperties;

    public EmrServerlessPlatformExecutor(EmrServerlessClient emrClient,
            InstanceProperties instanceProperties) {
        this.emrClient = emrClient;
        this.instanceProperties = instanceProperties;
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        String clusterName = String.join("-",
                instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME), "job",
                arguments.getJobRunId());
        String logUri = null == bulkImportBucket ? null
                : "s3://" + clusterName + "/emr-serverless/logs";

        StartJobRunRequest job = StartJobRunRequest.builder()
                .applicationId(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID))
                .name(clusterName + arguments.getJobRunId())
                .executionRoleArn(
                        instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN))
                .jobDriver(JobDriver.builder().sparkSubmit(SparkSubmit.builder()
                        .entryPoint("/workdir/bulk-import-runner.jar").entryPointArguments("1")
                        .sparkSubmitParameters(constructArgs(instanceProperties)).build()).build())
                .configurationOverrides(
                        ConfigurationOverrides.builder()
                                .monitoringConfiguration(MonitoringConfiguration.builder()
                                        .s3MonitoringConfiguration(S3MonitoringConfiguration
                                                .builder().logUri(logUri).build())
                                        .build())
                                .build())
                .build();

        StartJobRunResponse response = emrClient.startJobRun(job);
        LOGGER.info("Job {} started on application {}", response.jobRunId(),
                response.applicationId());
    }

    private String constructArgs(InstanceProperties instanceProperties) {
        String javaHome = instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME);

        return "--class " + instanceProperties.get(BULK_IMPORT_CLASS_NAME)
                + " --conf spark.executorEnv.JAVA_HOME=" + javaHome
                + " --conf spark.emr-serverless.driverEnv.JAVA_HOME=" + javaHome
                + " --conf spark.executor.cores=" + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES)
                + " --conf spark.executor.memory=" + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY)
                + " --conf spark.executor.instances=" + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES)
                + " --conf spark.driver.cores=" + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES)
                + " --conf spark.driver.memory=" + instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY);
    }
}
