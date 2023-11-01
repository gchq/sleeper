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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class AddTableIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key1");
    private final TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDB);
    private final TablePropertiesStore propertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    private final Configuration configuration = getHadoopConfiguration(localStackContainer);
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        new S3StateStoreCreator(instanceProperties, dynamoDB).create();
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
    }

    @Test
    void shouldAddTableWithNoPredefinedSplitPoints() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema, S3StateStore.class.getName());

        // When
        addTable(tableProperties);

        // Then
        TableIdentity foundId = tableIndex.getTableByName(tableProperties.get(TABLE_NAME)).orElseThrow();
        TableProperties foundProperties = propertiesStore.loadProperties(foundId);
        assertThat(foundProperties.get(TABLE_ID))
                .isNotEmpty().isEqualTo(foundId.getTableUniqueId());
        StateStore stateStore = new S3StateStore(instanceProperties, foundProperties, dynamoDB, configuration);
        assertThat(stateStore.getAllPartitions())
                .containsExactlyElementsOf(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .buildList());
    }

    @Test
    void shouldFailToAddTableIfTableAlreadyExists() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema, S3StateStore.class.getName());
        addTable(tableProperties);

        // When / Then
        assertThatThrownBy(() -> addTable(tableProperties))
                .isInstanceOf(TableAlreadyExistsException.class);
    }

    @Test
    void shouldAddTableWithSplitPoints() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema, S3StateStore.class.getName());
        Files.writeString(tempDir.resolve("splitpoints.txt"), "100");
        tableProperties.set(TableProperty.SPLIT_POINTS_FILE, tempDir.resolve("splitpoints.txt").toString());

        // When
        addTable(tableProperties);

        // Then
        StateStore stateStore = new S3StateStore(instanceProperties, tableProperties, dynamoDB, configuration);
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
