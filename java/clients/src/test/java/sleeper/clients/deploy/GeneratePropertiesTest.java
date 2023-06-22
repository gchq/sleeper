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

package sleeper.clients.deploy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.SchemaSerDe;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.GenerateInstanceProperties.generateTearDownDefaultsFromInstanceId;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class GeneratePropertiesTest {
    @Test
    void shouldGenerateInstancePropertiesCorrectly() {
        // Given/When
        InstanceProperties properties = generateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().generate();

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.set(ID, "test-instance");
        expected.set(CONFIG_BUCKET, "sleeper-test-instance-config");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(QUERY_RESULTS_BUCKET, "sleeper-test-instance-query-results");
        expected.set(VPC_ID, "some-vpc");
        expected.set(SUBNETS, "some-subnet");
        expected.set(ECR_COMPACTION_REPO, "test-instance/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-instance/ingest");
        expected.set(BULK_IMPORT_REPO, "test-instance/bulk-import-runner");
        expected.set(ACCOUNT, "test-account-id");
        expected.set(REGION, "aws-global");

        assertThat(properties).isEqualTo(expected);
    }

    @Test
    void shouldGenerateTearDownDefaultInstancePropertiesCorrectly() {
        // Given/When
        InstanceProperties properties = generateTearDownDefaultsFromInstanceId("test-instance");

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.set(ID, "test-instance");
        expected.set(CONFIG_BUCKET, "sleeper-test-instance-config");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(QUERY_RESULTS_BUCKET, "sleeper-test-instance-query-results");
        expected.set(ECR_COMPACTION_REPO, "test-instance/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-instance/ingest");
        expected.set(BULK_IMPORT_REPO, "test-instance/bulk-import-runner");
        expected.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-instance-CompactionJobCreationRule");
        expected.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-instance-CompactionTasksCreationRule");
        expected.set(SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-instance-SplittingCompactionTasksCreationRule");
        expected.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, "test-instance-FindPartitionsToSplitPeriodicTrigger");
        expected.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-instance-GarbageCollectorPeriodicTrigger");
        expected.set(INGEST_CLOUDWATCH_RULE, "test-instance-IngestTasksCreationRule");
        expected.set(INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE, "test-instance-IngestBatcherJobCreationRule");

        assertThat(properties).isEqualTo(expected);
    }

    @Test
    void shouldGenerateTablePropertiesCorrectly() {
        // Given
        InstanceProperties instanceProperties = generateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().generate();
        TableProperties tableProperties = GenerateTableProperties.from(instanceProperties,
                new SchemaSerDe().toJson(schemaWithKey("key")),
                new Properties(),
                "test-table");

        // Then
        TableProperties expected = new TableProperties(instanceProperties);
        expected.setSchema(schemaWithKey("key"));
        expected.set(TABLE_NAME, "test-table");
        expected.set(DATA_BUCKET, "sleeper-test-instance-table-test-table");

        assertThat(tableProperties).isEqualTo(expected);
    }

    @Test
    void shouldRetainWhitespaceInSchema() {
        // Given
        InstanceProperties instanceProperties = generateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().generate();
        String schemaWithNewlines = "{\"rowKeyFields\":[{\n" +
                "\"name\":\"key\",\"type\":\"LongType\"\n" +
                "}],\n" +
                "\"sortKeyFields\":[],\n" +
                "\"valueFields\":[]}";
        TableProperties tableProperties = GenerateTableProperties.from(instanceProperties,
                schemaWithNewlines,
                new Properties(),
                "test-table");

        // Then
        TableProperties expected = new TableProperties(instanceProperties);
        expected.setSchema(schemaWithKey("key"));
        expected.set(SCHEMA, schemaWithNewlines);
        expected.set(TABLE_NAME, "test-table");
        expected.set(DATA_BUCKET, "sleeper-test-instance-table-test-table");

        assertThat(tableProperties).isEqualTo(expected);
    }

    private GenerateInstanceProperties.Builder generateInstancePropertiesBuilder() {
        return GenerateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL);
    }
}
