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

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrserverless.model.JobDriver;
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.util.List;

import static sleeper.configuration.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_EXECUTOR_HEARTBEAT_INTERVAL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME;

/**
 * A {@link PlatformExecutor} which runs a bulk import job on EMR Serverless.
 */
public class EmrServerlessPlatformExecutor implements PlatformExecutor {
    private final EmrServerlessClient emrClient;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;

    public EmrServerlessPlatformExecutor(EmrServerlessClient emrClient,
            InstanceProperties instanceProperties) {
        this(emrClient, instanceProperties,
            new TablePropertiesProvider(AmazonS3ClientBuilder.defaultClient(), instanceProperties));
    }

    public EmrServerlessPlatformExecutor(EmrServerlessClient emrClient,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider) {
        this.emrClient = emrClient;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        TableProperties tableProperties = tablePropertiesProvider
                .getTableProperties(bulkImportJob.getTableName());
        BulkImportPlatformSpec platformSpec = new BulkImportPlatformSpec(tableProperties,
                bulkImportJob);
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        String clusterName = instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME);
        String jobName = String.join("-", "job", arguments.getJobRunId());
        String logUri = bulkImportBucket.isEmpty() ? "s3://" + clusterName
                : "s3://" + bulkImportBucket;

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
                .jobDriver(JobDriver.builder().sparkSubmit(SparkSubmit.builder()
                        .entryPoint("/workdir/bulk-import-runner.jar")
                        .entryPointArguments(instanceProperties.get(CONFIG_BUCKET),
                                bulkImportJob.getId(), taskId, arguments.getJobRunId())
                        .sparkSubmitParameters(
                                constructSparkArgs(taskId, arguments, bulkImportJob, platformSpec))
                        .build()).build())
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

    private String constructSparkArgs(String taskId, BulkImportArguments arguments,
            BulkImportJob bulkImportJob, BulkImportPlatformSpec platformSpec) {
        List<String> args = arguments.constructArgs(bulkImportJob, taskId);

        String javaHome = platformSpec.get(BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME);

        args.add("--class " + instanceProperties.get(BULK_IMPORT_CLASS_NAME));
        args.add("--conf spark.executorEnv.JAVA_HOME=" + javaHome);
        args.add("--conf spark.emr-serverless.driverEnv.JAVA_HOME=" + javaHome);
        args.add("--conf spark.emr-serverless.executor.disk="
                + platformSpec.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK));
        args.add("--conf spark.executor.cores="
                + platformSpec.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES));
        args.add("--conf spark.executor.memory="
                + platformSpec.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY));
        args.add("--conf spark.executor.instances="
                + platformSpec.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES));
        args.add("--conf spark.executor.heartbeat.interval=" + instanceProperties
                .get(BULK_IMPORT_EMR_SERVERLESS_SPARK_EXECUTOR_HEARTBEAT_INTERVAL));
        args.add("--conf spark.driver.cores="
                + platformSpec.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES));
        args.add("--conf spark.driver.memory="
                + platformSpec.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY));
        args.add("--conf spark.dynamicAllocation.enabled="
                + platformSpec.getBoolean(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION));
        return args.toString();
    }
}
