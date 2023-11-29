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

package sleeper.systemtest.drivers.instance;

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
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.statestore.s3.S3StateStoreCreator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class SleeperInstanceTablesDriverIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(S3, DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, S3, AmazonS3ClientBuilder.standard());
    private final S3Client s3v2 = buildAwsV2Client(localStackContainer, S3, S3Client.builder());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final Configuration hadoop = getHadoopConfiguration(localStackContainer);
    private final SleeperInstanceTablesDriver driver = new SleeperInstanceTablesDriver(s3, s3v2, dynamoDB, hadoop);
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDB).create();
        new S3StateStoreCreator(instanceProperties, dynamoDB).create();
    }

    @Test
    void shouldAddOneTable() {
        driver.add(instanceProperties, tableProperties);
        assertThat(driver.tableIndex(instanceProperties).streamAllTables())
                .containsExactly(tableProperties.getId());
    }

    @Test
    void shouldInitialiseTablePartitions() throws StateStoreException {
        driver.add(instanceProperties, tableProperties);
        assertThat(driver.createStateStoreProvider(instanceProperties).getStateStore(tableProperties)
                .getAllPartitions())
                .hasSize(1);
    }

    @Test
    void shouldDeleteOneTable() {
        driver.add(instanceProperties, tableProperties);
        driver.deleteAll(instanceProperties);
        assertThat(driver.tableIndex(instanceProperties).streamAllTables())
                .isEmpty();
    }

    @Test
    void shouldDeleteNothingWhenNoTablesArePresent() {
        assertThatCode(() -> driver.deleteAll(instanceProperties))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldDeleteNothingWhenNoTablesArePresentAndInstancePropertiesAreSavedInConfigBucket() {
        instanceProperties.saveToS3(s3);
        driver.deleteAll(instanceProperties);

        InstanceProperties loaded = new InstanceProperties();
        loaded.loadFromS3GivenInstanceId(s3, instanceProperties.get(ID));
        assertThat(loaded).isEqualTo(instanceProperties);
    }
}
