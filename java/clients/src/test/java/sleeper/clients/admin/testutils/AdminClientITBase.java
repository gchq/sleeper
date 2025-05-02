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
package sleeper.clients.admin.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.AdminClient;
import sleeper.clients.admin.AdminClientTrackerFactory;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.localstack.test.SleeperLocalStackClients;
import sleeper.task.common.QueueMessageCount;

import java.nio.file.Path;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;

public abstract class AdminClientITBase extends AdminClientTestBase {

    protected final AmazonS3 s3 = SleeperLocalStackClients.S3_CLIENT;
    protected final AmazonDynamoDB dynamoDB = SleeperLocalStackClients.DYNAMO_CLIENT;
    protected final InvokeCdkForInstance cdk = mock(InvokeCdkForInstance.class);
    protected final UploadDockerImages uploadDockerImages = mock(UploadDockerImages.class);
    protected TablePropertiesStore tablePropertiesStore;

    @TempDir
    protected Path tempDir;

    @Override
    public void startClient(AdminClientTrackerFactory trackers, QueueMessageCount.Client queueClient) throws InterruptedException {
        AdminClient.start(instanceId, s3, dynamoDB, cdk, tempDir, uploadDockerImages,
                out.consoleOut(), in.consoleIn(), editor, queueClient, properties -> Map.of());
    }

    protected AdminClientPropertiesStore store() {
        return storeWithGeneratedDirectory(tempDir);
    }

    protected AdminClientPropertiesStore storeWithGeneratedDirectory(Path path) {
        return new AdminClientPropertiesStore(s3, dynamoDB, cdk, path, uploadDockerImages);
    }

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        instanceId = instanceProperties.get(ID);
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3, dynamoDB);
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        tablePropertiesStore.save(tableProperties);
    }
}
