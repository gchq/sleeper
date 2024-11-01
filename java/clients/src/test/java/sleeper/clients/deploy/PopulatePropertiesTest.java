/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.clients.deploy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.PopulateInstancePropertiesAws.generateTearDownDefaultsFromInstanceId;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WARM_LAMBDA_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_RULE;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.core.properties.instance.IngestProperty.ECR_INGEST_REPO;

class PopulatePropertiesTest {

    private PopulateInstancePropertiesAws.Builder populateInstancePropertiesBuilder() {
        return PopulateInstancePropertiesAws.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL)
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet");
    }

    private InstanceProperties expectedInstanceProperties() {
        InstanceProperties expected = new InstanceProperties();
        expected.setTags(Map.of("InstanceID", "test-instance"));
        expected.set(ID, "test-instance");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(VPC_ID, "some-vpc");
        expected.set(SUBNETS, "some-subnet");
        expected.set(ECR_COMPACTION_REPO, "test-instance/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-instance/ingest");
        expected.set(BULK_IMPORT_REPO, "test-instance/bulk-import-runner");
        expected.set(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, "test-instance/bulk-import-runner-emr-serverless");
        expected.set(ACCOUNT, "test-account-id");
        expected.set(REGION, "aws-global");
        return expected;
    }

    @Test
    void shouldPopulateInstanceProperties() {
        // Given/When
        InstanceProperties properties = populateInstancePropertiesBuilder().build().populate();

        // Then
        assertThat(properties).isEqualTo(expectedInstanceProperties());
    }

    @Test
    void shouldApplyECRRepositoryPrefixFromInstancePropertiesTemplate() {
        // Given
        InstanceProperties template = new InstanceProperties();
        template.set(ECR_REPOSITORY_PREFIX, "test-ecr-prefix");

        // When
        InstanceProperties properties = populateInstancePropertiesBuilder()
                .instanceProperties(template)
                .build().populate();

        // Then
        InstanceProperties expected = expectedInstanceProperties();
        expected.set(ECR_REPOSITORY_PREFIX, "test-ecr-prefix");
        expected.set(ECR_COMPACTION_REPO, "test-ecr-prefix/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-ecr-prefix/ingest");
        expected.set(BULK_IMPORT_REPO, "test-ecr-prefix/bulk-import-runner");
        expected.set(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, "test-ecr-prefix/bulk-import-runner-emr-serverless");
        assertThat(properties).isEqualTo(expected);
    }

    @Test
    void shouldGetDefaultTagsWhenNotProvidedAndNotSetInInstanceProperties() {
        // Given/When
        InstanceProperties properties = populateInstancePropertiesBuilder().build().populate();

        // Then
        assertThat(properties.getTags())
                .isEqualTo(Map.of("InstanceID", "test-instance"));
    }

    @Test
    void shouldAddToExistingTagsWhenSetInInstanceProperties() {
        // Given/When
        InstanceProperties beforePopulate = new InstanceProperties();
        beforePopulate.setTags(Map.of("TestTag", "TestValue"));
        InstanceProperties afterPopulate = populateInstancePropertiesBuilder()
                .instanceProperties(beforePopulate)
                .build().populate();

        // Then
        assertThat(afterPopulate.getTags())
                .isEqualTo(Map.of("TestTag", "TestValue",
                        "InstanceID", "test-instance"));
    }

    @Test
    void shouldGenerateDefaultInstancePropertiesFromInstanceId() {
        // Given/When
        InstanceProperties properties = generateTearDownDefaultsFromInstanceId("test-instance");

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.set(ID, "test-instance");
        expected.set(CONFIG_BUCKET, "sleeper-test-instance-config");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(QUERY_RESULTS_BUCKET, "sleeper-test-instance-query-results");
        expected.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
        expected.set(ECR_COMPACTION_REPO, "test-instance/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-instance/ingest");
        expected.set(BULK_IMPORT_REPO, "test-instance/bulk-import-runner");
        expected.set(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, "test-instance/bulk-import-runner-emr-serverless");
        expected.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-instance-CompactionJobCreationRule");
        expected.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-instance-CompactionTasksCreationRule");
        expected.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, "test-instance-FindPartitionsToSplitPeriodicTrigger");
        expected.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-instance-GarbageCollectorPeriodicTrigger");
        expected.set(INGEST_CLOUDWATCH_RULE, "test-instance-IngestTasksCreationRule");
        expected.set(INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE, "test-instance-IngestBatcherJobCreationRule");
        expected.set(TABLE_METRICS_RULE, "test-instance-MetricsPublishRule");
        expected.set(QUERY_WARM_LAMBDA_CLOUDWATCH_RULE, "test-instance-QueryWarmLambdaRule");
        expected.set(TRANSACTION_LOG_SNAPSHOT_CREATION_RULE, "test-instance-TransactionLogSnapshotCreationRule");
        expected.set(TRANSACTION_LOG_SNAPSHOT_DELETION_RULE, "test-instance-TransactionLogSnapshotDeletionRule");
        expected.set(TRANSACTION_LOG_TRANSACTION_DELETION_RULE, "test-instance-TransactionLogTransactionDeletionRule");

        assertThat(properties).isEqualTo(expected);
    }
}
