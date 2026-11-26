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
package sleeper.bulkimport.starter.executor;

import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.Configuration;
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrserverless.model.JobDriver;
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;

import sleeper.bulkimport.core.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_OPTIMIZATION;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION;

/**
 * Starts a bulk import job on EMR Serverless.
 */
public class EmrServerlessPlatformExecutor implements PlatformExecutor {
    private final EmrServerlessClient emrClient;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;

    public EmrServerlessPlatformExecutor(EmrServerlessClient emrClient,
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        this.emrClient = emrClient;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        TableProperties tableProperties = tablePropertiesProvider.getByName(bulkImportJob.getTableName());
        BulkImportPlatformSpec platformSpec = new BulkImportPlatformSpec(tableProperties, bulkImportJob);
        StartJobRunRequest job = StartJobRunRequest.builder()
                .applicationId(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID))
                .name(String.join("-", "job", arguments.getJobRunId()))
                .executionRoleArn(
                        instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN))
                .jobDriver(JobDriver.builder().sparkSubmit(SparkSubmit.builder()
                        .entryPoint("s3://" + instanceProperties.get(JARS_BUCKET) + "/bulk-import-runner-" + instanceProperties.get(VERSION) + ".jar")
                        .entryPointArguments(arguments.entryPointArgumentsForServerless())
                        .sparkSubmitParameters(arguments.sparkSubmitParametersForServerless(platformSpec))
                        .build()).build())
                .configurationOverrides(configurationOverrides(platformSpec))
                .build();
        emrClient.startJobRun(job);
    }

    /**
     * Creates the configuration overrides for a job. This sets where logs are written. It also turns on EMR Serverless
     * dynamic allocation optimization if dynamic allocation is enabled for the job. That setting must be set in the
     * "spark" classification of the application configuration, rather than as Spark configuration.
     *
     * @param  platformSpec the platform spec for the job, including the table properties
     * @return              the configuration overrides
     */
    private ConfigurationOverrides configurationOverrides(BulkImportPlatformSpec platformSpec) {
        ConfigurationOverrides.Builder builder = ConfigurationOverrides.builder()
                .monitoringConfiguration(MonitoringConfiguration.builder()
                        .s3MonitoringConfiguration(S3MonitoringConfiguration.builder()
                                .logUri("s3://" + instanceProperties.get(BULK_IMPORT_BUCKET))
                                .build())
                        .build());
        if (platformSpec.getBoolean(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION)
                && instanceProperties.getBoolean(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION_OPTIMIZATION)) {
            builder.applicationConfiguration(Configuration.builder()
                    .classification("spark")
                    .properties(Map.of("dynamicAllocationOptimization", "true"))
                    .build());
        }
        return builder.build();
    }
}
