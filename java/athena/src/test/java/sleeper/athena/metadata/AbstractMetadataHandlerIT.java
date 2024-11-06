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
package sleeper.athena.metadata;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.athena.TestUtils;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;

import java.io.IOException;
import java.nio.file.Path;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;
import static java.nio.file.Files.createTempDirectory;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public abstract class AbstractMetadataHandlerIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    // For storing data
    @TempDir
    public static Path tempDir;

    protected static final Schema TIME_SERIES_SCHEMA = Schema.builder()
            .rowKeyFields(
                    new Field("year", new IntType()),
                    new Field("month", new IntType()),
                    new Field("day", new IntType()))
            .valueFields(new Field("count", new LongType()))
            .build();

    protected final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final Configuration configuration = getHadoopConfiguration(localStackContainer);

    @BeforeEach
    public void setUpCredentials() {
        // Annoyingly the MetadataHandler hard-codes the S3 client it uses to check the spill bucket. Therefore
        // I need to set up some credentials in System properties so the default client will pick them up.
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, localStackContainer.getAccessKey());
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, localStackContainer.getSecretKey());
        System.setProperty(AWS_REGION_SYSTEM_PROPERTY, localStackContainer.getRegion());
    }

    @AfterEach
    public void clearUpCredentials() {
        System.clearProperty(ACCESS_KEY_SYSTEM_PROPERTY);
        System.clearProperty(SECRET_KEY_SYSTEM_PROPERTY);
        System.clearProperty(AWS_REGION_SYSTEM_PROPERTY);
        s3Client.shutdown();
        dynamoClient.shutdown();
    }

    protected InstanceProperties createInstance() throws IOException {
        return TestUtils.createInstance(s3Client, dynamoClient,
                createTempDirectory(tempDir, null).toString());
    }

    protected TableProperties createEmptyTable(InstanceProperties instanceProperties) {
        return TestUtils.createTable(instanceProperties, TIME_SERIES_SCHEMA,
                dynamoClient, s3Client, getHadoopConfiguration(localStackContainer), 2018, 2019, 2020);
    }

    protected TableProperties createTable(InstanceProperties instanceProperties) throws IOException {
        TableProperties table = createEmptyTable(instanceProperties);
        TestUtils.ingestData(s3Client, dynamoClient, createTempDirectory(tempDir, null).toString(),
                instanceProperties, table);
        return table;
    }
}
