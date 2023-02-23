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

package sleeper.clients.admin.deploy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class GeneratePropertiesTest {
    @Test
    void shouldGenerateInstancePropertiesCorrectly() {
        // Given/When
        InstanceProperties properties = generateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetId("some-subnet")
                .build().generate();

        // Then
        assertThat(properties.get(ID)).isEqualTo("test-instance");
        assertThat(properties.get(CONFIG_BUCKET)).isEqualTo("sleeper-test-instance-config");
        assertThat(properties.get(JARS_BUCKET)).isEqualTo("sleeper-test-instance-jars");
        assertThat(properties.get(QUERY_RESULTS_BUCKET)).isEqualTo("sleeper-test-instance-query-results");
        assertThat(properties.get(VPC_ID)).isEqualTo("some-vpc");
        assertThat(properties.get(SUBNET)).isEqualTo("some-subnet");
        assertThat(properties.get(ECR_COMPACTION_REPO)).isEqualTo("test-instance/compaction-job-execution");
        assertThat(properties.get(ECR_INGEST_REPO)).isEqualTo("test-instance/ingest");
        assertThat(properties.get(BULK_IMPORT_REPO)).isEqualTo("test-instance/bulk-import-runner");
        // Should not set sleeper version (system defined)
        assertThat(properties.get(VERSION)).isNull();
    }

    @Test
    void shouldGenerateTablePropertiesCorrectly() {
        // Given
        InstanceProperties instanceProperties = generateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetId("some-subnet")
                .build().generate();
        TableProperties tableProperties = GenerateTableProperties.from(instanceProperties, schemaWithKey("key"), "test-table");

        // Then
        assertThat(tableProperties.get(TABLE_NAME)).isEqualTo("test-table");
        assertThat(tableProperties.get(DATA_BUCKET)).isEqualTo("sleeper-test-instance-table-test-table");
    }

    private GenerateInstanceProperties.Builder generateInstancePropertiesBuilder() {
        return GenerateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL);
    }
}
