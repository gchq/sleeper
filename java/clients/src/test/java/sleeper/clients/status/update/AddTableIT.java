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

package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class AddTableIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.createStore(instanceProperties, s3, dynamoDB);
    private final Configuration configuration = getHadoopConfiguration(localStackContainer);
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
    }

    @Test
    void shouldAddTableWithNoPredefinedSplitPoints() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

        // When
        addTable(tableProperties);

        // Then
        TableProperties foundProperties = propertiesStore.loadByName(tableProperties.get(TABLE_NAME));
        assertThat(foundProperties).isEqualTo(tableProperties);
        assertThat(foundProperties.get(TABLE_ID)).isNotEmpty();
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3, dynamoDB, configuration).getStateStore(foundProperties);
        assertThat(stateStore.getAllPartitions())
                .containsExactlyElementsOf(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .buildList());
    }

    @Test
    void shouldFailToAddTableIfTableAlreadyExists() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        addTable(tableProperties);

        // When / Then
        assertThatThrownBy(() -> addTable(tableProperties))
                .isInstanceOf(TableAlreadyExistsException.class);
    }

    @Test
    void shouldFailToAddTableIfNameNotSet() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.unset(TABLE_NAME);

        // When / Then
        assertThatThrownBy(() -> addTable(tableProperties))
                .isInstanceOf(SleeperPropertiesInvalidException.class);
    }

    @Test
    void shouldAddTableWithSplitPoints() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        Files.writeString(tempDir.resolve("splitpoints.txt"), "100");
        tableProperties.set(TableProperty.SPLIT_POINTS_FILE, tempDir.resolve("splitpoints.txt").toString());

        // When
        addTable(tableProperties);

        // Then
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3, dynamoDB, configuration).getStateStore(tableProperties);
        assertThat(stateStore.getAllPartitions())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 100L)
                        .buildList());
    }

    private void addTable(TableProperties tableProperties) throws IOException {
        new AddTable(s3, dynamoDB, instanceProperties, tableProperties, configuration).run();
    }
}
