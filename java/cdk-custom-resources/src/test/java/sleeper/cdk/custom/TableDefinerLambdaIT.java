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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableNotFoundException;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class TableDefinerLambdaIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
    private final TableDefinerLambda tableDefinerLambda = new TableDefinerLambda(s3Client, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private String inputFolderName;

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        inputFolderName = createTempDirectory(tempDir, null).toString();
    }

    @Test
    public void shouldCreateTableWithNoSplitPoints() throws IOException {
        // Given
        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("tableProperties", tableProperties.saveAsString());
        resourceProperties.put("splitPoints", "");

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        // When
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

    @Test
    public void shouldCreateTableWithMultipleSplitPoints() throws IOException {
        // Given
        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("tableProperties", tableProperties.saveAsString());
        resourceProperties.put("splitPoints", "0\n5\n10\n");

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(resourceProperties)
                .build();

        // When
        tableDefinerLambda.handleEvent(event, null);

        // Then
        TableProperties foundProperties = propertiesStore.loadByName(tableProperties.get(TABLE_NAME));
        assertThat(foundProperties).isEqualTo(tableProperties);
        assertThat(foundProperties.get(TABLE_ID)).isNotEmpty();
        assertThat(stateStore(foundProperties).getAllPartitions())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "l", "r", Long.valueOf(5))
                        .splitToNewChildren("l", "ll", "lr", Long.valueOf(0))
                        .splitToNewChildren("r", "rl", "rr", Long.valueOf(10))
                        .buildList());
    }

    @Test
    public void shouldFailToDeleteTableThatDoesNotExist() {
        // When / Then
        HashMap<String, Object> resourceProperties = new HashMap<>();
        resourceProperties.put("tableProperties", tableProperties.saveAsString());

        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(resourceProperties)
                .build();

        // When
        assertThatThrownBy(() -> tableDefinerLambda.handleEvent(event, null))
                .isInstanceOf(TableNotFoundException.class);

    }

    private StateStore stateStore(TableProperties tableProperties) {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }
}
