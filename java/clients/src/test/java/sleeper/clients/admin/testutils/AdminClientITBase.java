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
package sleeper.clients.admin.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.clients.AdminClient;
import sleeper.clients.admin.AdminClientStatusStoreFactory;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.deploy.UploadDockerImages;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.task.common.QueueMessageCount;

import java.nio.file.Path;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public abstract class AdminClientITBase extends AdminClientTestBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    protected final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final InvokeCdkForInstance cdk = mock(InvokeCdkForInstance.class);
    protected final UploadDockerImages uploadDockerImages = mock(UploadDockerImages.class);
    protected TablePropertiesStore tablePropertiesStore;

    @TempDir
    protected Path tempDir;

    @Override
    public void startClient(AdminClientStatusStoreFactory statusStores, QueueMessageCount.Client queueClient) throws InterruptedException {
        AdminClient.start(instanceId, s3, dynamoDB, cdk, tempDir, uploadDockerImages,
                out.consoleOut(), in.consoleIn(), editor, queueClient, properties -> Map.of());
    }

    protected AdminClientPropertiesStore store() {
        return storeWithGeneratedDirectory(tempDir);
    }

    protected AdminClientPropertiesStore storeWithGeneratedDirectory(Path path) {
        return new AdminClientPropertiesStore(s3, dynamoDB, cdk, path, uploadDockerImages);
    }

    @AfterEach
    public void tearDownITBase() {
        s3.shutdown();
    }

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        instanceId = instanceProperties.get(ID);
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        tablePropertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        tablePropertiesStore.save(tableProperties);
    }
}
