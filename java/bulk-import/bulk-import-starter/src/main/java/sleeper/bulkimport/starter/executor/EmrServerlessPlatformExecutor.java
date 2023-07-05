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
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;

import sleeper.configuration.properties.InstanceProperties;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;

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
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        String clusterName = String.join("-", "sleeper", "emr", "serverless");
        String logUri = null == bulkImportBucket ? null : "s3://" + clusterName + "/emr-serverless/logs";

        StartJobRunRequest job = StartJobRunRequest.builder()
                .applicationId(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID))
                .name(clusterName + arguments.getJobRunId())
                .executionRoleArn("roleId")  //Todo Role that can run job
                .tags(instanceProperties.getTags())
                .jobDriver(JobDriver.builder()
                        //.sparkSubmit(null) //Todo Spark Job
                        .build())
                .configurationOverrides(ConfigurationOverrides.builder()
                        .monitoringConfiguration(MonitoringConfiguration.builder()
                                .s3MonitoringConfiguration(S3MonitoringConfiguration
                                        .builder()
                                        .logUri(logUri)
                                        .build())
                                .build())
                        .build())
                .build();

        emrClient.startJobRun(job);
    }
}
