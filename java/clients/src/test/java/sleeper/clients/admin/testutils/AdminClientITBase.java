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
package sleeper.clients.admin.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.deploy.UploadDockerImages;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TablePropertiesStore;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.CommonTestConstants;

import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public abstract class AdminClientITBase extends AdminClientTestBase {

    protected static final String CONFIG_BUCKET_NAME = "sleeper-" + INSTANCE_ID + "-config";

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    protected final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final InvokeCdkForInstance cdk = mock(InvokeCdkForInstance.class);
    protected final UploadDockerImages uploadDockerImages = mock(UploadDockerImages.class);
    private TablePropertiesStore tablePropertiesStore;

    @TempDir
    protected Path tempDir;

    @Override
    public AdminClientPropertiesStore getStore() {
        return storeWithGeneratedDirectory(tempDir);
    }

    protected AdminClientPropertiesStore store() {
        return getStore();
    }

    protected AdminClientPropertiesStore storeWithGeneratedDirectory(Path path) {
        return new AdminClientPropertiesStore(s3, dynamoDB, cdk, path, uploadDockerImages);
    }

    @BeforeEach
    public void setUpITBase() {
        s3.createBucket(CONFIG_BUCKET_NAME);
    }

    @AfterEach
    public void tearDownITBase() {
        S3Objects.inBucket(s3, CONFIG_BUCKET_NAME)
                .forEach(object -> s3.deleteObject(CONFIG_BUCKET_NAME, object.getKey()));
        s3.deleteBucket(CONFIG_BUCKET_NAME);
        s3.shutdown();
    }

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        instanceProperties.saveToS3(s3);
        tablePropertiesStore = new S3TablePropertiesStore(instanceProperties, s3, dynamoDB);
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        tablePropertiesStore.save(tableProperties);
    }
}
