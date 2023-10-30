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

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.validation.EmrInstanceArchitecture;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

/**
 * A {@link PlatformExecutor} which runs a bulk import job on EMR Serverless.
 */
public class EmrServerlessPlatformExecutor implements PlatformExecutor {
    private final EmrServerlessClient emrClient;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;

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
                .getByName(bulkImportJob.getTableName());
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
        Map<String, String> instancePropertiesSparkConf = ConfigurationUtils
                .getSparkServerlessConfigurationFromInstanceProperties(instanceProperties,
                        EmrInstanceArchitecture.X86_64);

        Map<String, String> jobSparkArgs = arguments.getBulkImportJob().getSparkConf();
        if (jobSparkArgs != null) {
            jobSparkArgs = jobSparkArgs.entrySet()
                    .stream().filter(prop -> prop.getKey().contains("sleeper.bulk.import.emr.serverless.spark"))
                    .map(i -> Map.entry(
                            i.getKey().split("sleeper\\.bulk\\.import\\.emr\\.serverless\\.")[1],
                            i.getValue()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            Map<String, String> sparkConf = Stream
                    .of(instancePropertiesSparkConf, jobSparkArgs)
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            Map.Entry::getValue, (m1, m2) -> m2, HashMap::new));
            bulkImportJob = bulkImportJob.toBuilder().sparkConf(sparkConf).build();
        }

        List<String> args = arguments.constructArgs(bulkImportJob, taskId);

        StringBuilder argsAsString = new StringBuilder();
        args.forEach(arg -> {
            argsAsString.append(arg).append(" ");
        });
        return argsAsString.toString();
    }
}
