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

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.ApplicationSummary;
import software.amazon.awssdk.services.emrserverless.model.Architecture;
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationRequest;
import software.amazon.awssdk.services.emrserverless.model.ImageConfigurationInput;
import software.amazon.awssdk.services.emrserverless.model.JobDriver;
import software.amazon.awssdk.services.emrserverless.model.ListApplicationsRequest;
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.statestore.StateStoreProvider;

import java.util.Map;
import java.util.UUID;

//import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_RELEASE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_TYPE;

/**
 * An {@link Executor} which runs a bulk import job on an EMR cluster.
 */
public class EmrServerlessExecutor extends AbstractEmrExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrServerlessExecutor.class);
    private final EmrServerlessClient emrClient;
    private String clusterName;

    public EmrServerlessExecutor(EmrServerlessClient emrClient,
                       InstanceProperties instanceProperties,
                       TablePropertiesProvider tablePropertiesProvider,
                       StateStoreProvider stateStoreProvider, AmazonS3 amazonS3) {
        super(instanceProperties, tablePropertiesProvider, stateStoreProvider, amazonS3);
        this.emrClient = emrClient;
    }

    @Override
    public void runJobOnPlatform(BulkImportJob bulkImportJob) {
        //String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        //String logUri = null == bulkImportBucket ? null : "s3://" + bulkImportBucket + "/logs";

        // Get Appliation Id to run the job, this find the first application where the cluster names match.
        clusterName = String.join("-", "sleeper", "emr", "serverless");
        String applicationId = "";
        for (ApplicationSummary summary : emrClient.listApplications(ListApplicationsRequest.builder().build()).applications()) {
                if (summary.name().equals(clusterName)) {
                        applicationId = summary.id();
                        break;
                }
        }
        if (applicationId.isEmpty()) {
                LOGGER.error("Unable to find {} application to run the jon in", BULK_IMPORT_EMR_SERVERLESS_TYPE);
                throw new RuntimeException("No Application found");
        }

        StartJobRunRequest job = StartJobRunRequest.builder()
                .applicationId(applicationId)
                .name(clusterName + UUID.randomUUID().toString())
                .executionRoleArn("applicationId")  //Todo Role that can run job
                .tags(instanceProperties.getTags())
                .jobDriver(JobDriver.builder()
                        //.sparkSubmit(null) //Todo Spark Job
                        .build())
                .configurationOverrides(ConfigurationOverrides.builder()
                        .monitoringConfiguration(MonitoringConfiguration.builder()
                                .s3MonitoringConfiguration(S3MonitoringConfiguration
                                        .builder()
                                        .logUri("s3://" + clusterName + "/emr-serverless/logs")
                                        .build())
                                .build())
                        .build())
                .build();

        emrClient.startJobRun(job);
    }

    protected String getFromPlatformSpec(TableProperty tableProperty, Map<String, String> platformSpec, TableProperties tableProperties) {
        if (null == platformSpec) {
            return tableProperties.get(tableProperty);
        }
        return platformSpec.getOrDefault(tableProperty.getPropertyName(), tableProperties.get(tableProperty));
    }

    // Not needed this is done via CDK
    public void createApplication() {
        if (!BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE.toString().equals(Architecture.ARM64.toString()) ||
        !BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE.toString().equals(Architecture.X86_64.toString())) {
                LOGGER.error("{} is not a valid Architecture. Use either X86_64 or ARM64",
                        BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE);
                throw new RuntimeException("Invalid Architecture deatcted for EMR Serverless");
        }
        CreateApplicationRequest appRequest = CreateApplicationRequest.builder()
        .name(clusterName)
        .architecture(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE.toString())
        .type(BULK_IMPORT_EMR_SERVERLESS_TYPE.toString())
        .releaseLabel(BULK_IMPORT_EMR_SERVERLESS_RELEASE.toString())
        .imageConfiguration(ImageConfigurationInput.builder()
                .imageUri(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO.toString())
                .build())
        .build();
        emrClient.createApplication(appRequest);
    }
}
