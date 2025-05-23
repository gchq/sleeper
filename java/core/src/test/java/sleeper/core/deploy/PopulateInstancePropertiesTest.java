/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.deploy;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.deploy.PopulatePropertiesTestHelper.createTestPopulateInstanceProperties;
import static sleeper.core.deploy.PopulatePropertiesTestHelper.testPopulateInstancePropertiesBuilder;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_TASK_CREATION_CLOUDWATCH_RULE;
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
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.PartitionSplittingProperty.DEFAULT_PARTITION_SPLIT_THRESHOLD;

public class PopulateInstancePropertiesTest {

    private InstanceProperties expectedInstanceProperties() {
        InstanceProperties expected = new InstanceProperties();
        expected.setTags(Map.of("InstanceID", "test-instance"));
        expected.set(ID, "test-instance");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(VPC_ID, "some-vpc");
        expected.set(SUBNETS, "some-subnet");
        expected.set(ACCOUNT, "test-account-id");
        expected.set(REGION, "test-region");
        return expected;
    }

    @Test
    void shouldPopulateInstanceProperties() {
        // Given/When
        InstanceProperties properties = createTestPopulateInstanceProperties().populate(new InstanceProperties());

        // Then
        assertThat(properties).isEqualTo(expectedInstanceProperties());
    }

    @Test
    void shouldGetDefaultTagsWhenNotProvidedAndNotSetInInstanceProperties() {
        // Given/When
        InstanceProperties properties = createTestPopulateInstanceProperties().populate(new InstanceProperties());

        // Then
        assertThat(properties.getTags())
                .isEqualTo(Map.of("InstanceID", "test-instance"));
    }

    @Test
    void shouldAddToExistingTagsWhenSetInInstanceProperties() {
        // Given/When
        InstanceProperties properties = new InstanceProperties();
        properties.setTags(Map.of("TestTag", "TestValue"));
        createTestPopulateInstanceProperties().populate(properties);

        // Then
        assertThat(properties.getTags())
                .isEqualTo(Map.of("TestTag", "TestValue",
                        "InstanceID", "test-instance"));
    }

    @Test
    void shouldSetExtraProperties() {
        // Given/When
        InstanceProperties properties = new InstanceProperties();
        testPopulateInstancePropertiesBuilder()
                .extraInstanceProperties(p -> p.setNumber(DEFAULT_PARTITION_SPLIT_THRESHOLD, 1000))
                .build().populate(properties);

        // Then
        assertThat(properties.getInt(DEFAULT_PARTITION_SPLIT_THRESHOLD))
                .isEqualTo(1000);
    }

    @Test
    void shouldGenerateDefaultInstancePropertiesFromInstanceId() {
        // Given/When
        InstanceProperties properties = PopulateInstanceProperties.generateTearDownDefaultsFromInstanceId("test-instance");

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.set(ID, "test-instance");
        expected.set(CONFIG_BUCKET, "sleeper-test-instance-config");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(QUERY_RESULTS_BUCKET, "sleeper-test-instance-query-results");
        expected.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
        expected.set(BULK_EXPORT_TASK_CREATION_CLOUDWATCH_RULE, "test-instance-BulkExportJobCreationRule");
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
