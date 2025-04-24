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
package sleeper.clients.api;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SleeperClientIT extends LocalStackTestBase {

    Schema schema = createSchemaWithKey("key", new StringType());

    @Test
    void shouldCreateAwsSleeperClient() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);

        AwsSleeperClientBuilder clientBuilder = new AwsSleeperClientBuilder();
        SleeperClient sleeperClient = clientBuilder.instanceId(instanceProperties.get(ID))
                .s3Client(s3Client)
                .dynamoClient(dynamoClient)
                .sqsClient(sqsClient)
                .hadoopConf(hadoopConf)
                .build();

        // When
        String tableName = "table-name";
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        sleeperClient.addTable(tableProperties, List.of());

        // Then
        assertThat(sleeperClient.doesTableExist(tableName)).isTrue();
    }
}
