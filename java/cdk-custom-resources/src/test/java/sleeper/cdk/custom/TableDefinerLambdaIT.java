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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class TableDefinerLambdaIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);

    private TableProperties createTableProperties(String bucketName) {
        Properties properties = new Properties();
        properties.setProperty(TABLE_NAME.getPropertyName(), "tableName");
        properties.setProperty(SCHEMA.getPropertyName(), schema.toString());
        return new TableProperties(instanceProperties, properties);
    }

    @Test
    public void shouldCreateTable() throws IOException {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        TableDefinerLambda tableDefinerLambda = new TableDefinerLambda(s3Client, dynamoClient, bucketName);

        // When
        TableProperties tableProperties = createTableProperties(bucketName);

        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("tableProperties", tableProperties.saveAsString());
        resourceProperties.put("splitPoints", "");

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        tableDefinerLambda.handleEvent(event, null);

        // Then
        TableProperties foundProperties = propertiesStore.loadByName(tableProperties.get(TABLE_NAME));
        assertThat(foundProperties).isEqualTo(tableProperties);
        assertThat(foundProperties.get(TABLE_ID)).isNotEmpty();
        assertThat(stateStore(foundProperties).getAllPartitions())
                .containsExactlyElementsOf(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .buildList());

    }

    private StateStore stateStore(TableProperties tableProperties) {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }
}
