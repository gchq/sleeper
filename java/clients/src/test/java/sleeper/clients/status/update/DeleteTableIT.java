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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableNotFoundException;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.s3.S3StateStoreCreator;

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
import static sleeper.core.table.TableIdentity.uniqueIdAndName;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class DeleteTableIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    private final Configuration conf = getHadoopConfiguration(localStackContainer);
    private final StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, conf);

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        S3StateStoreCreator stateStoreCreator = new S3StateStoreCreator(instanceProperties, dynamoDB);
        stateStoreCreator.create();
    }

    @Test
    void shouldDeleteTable() throws StateStoreException {
        // Given
        TableProperties table1 = createTable(uniqueIdAndName("test-table-1", "table-1"));
        StateStore stateStoreBefore = createStateStore(table1);
        stateStoreBefore.initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 100L)
                .buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStoreBefore);
        stateStoreBefore.addFile(factory.rootFile("file1.parquet", 123L));

        // When
        deleteTable("table-1");

        // Then
        assertThatThrownBy(() -> propertiesStore.findByName("table-1"))
                .isInstanceOf(TableNotFoundException.class);
        StateStore stateStoreAfter = stateStoreProvider.getStateStore(table1);
        assertThat(stateStoreAfter.getAllPartitions()).isEmpty();
        assertThat(stateStoreAfter.getFileReferences()).isEmpty();
    }

    @Test
    void shouldFailToDeleteTableThatDoesNotExist() {
        // When / Then
        assertThatThrownBy(() -> deleteTable("table-1"))
                .isInstanceOf(TableNotFoundException.class);
    }

    private void deleteTable(String tableName) {
        new DeleteTable(propertiesStore, stateStoreProvider).delete(tableName);
    }

    private TableProperties createTable(TableIdentity tableIdentity) {
        TableProperties table = createTestTableProperties(instanceProperties, schema);
        table.set(TABLE_ID, tableIdentity.getTableUniqueId());
        table.set(TABLE_NAME, tableIdentity.getTableName());
        propertiesStore.save(table);
        return table;
    }

    private StateStore createStateStore(TableProperties tableProperties) {
        return new S3StateStore(instanceProperties, tableProperties, dynamoDB, conf);
    }
}
