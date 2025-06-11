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

import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.admin.AdminClient;
import sleeper.clients.admin.AdminClientTrackerFactory;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.common.task.QueueMessageCount;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.configurationv2.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.table.TableIndex;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.localstack.test.SleeperLocalStackClients;

import java.nio.file.Path;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;

public abstract class AdminClientITBase extends AdminClientTestBase {

    protected final S3Client s3 = SleeperLocalStackClients.S3_CLIENT_V2;
    protected final DynamoDbClient dynamoDB = SleeperLocalStackClients.DYNAMO_CLIENT_V2;
    protected final InvokeCdkForInstance cdk = mock(InvokeCdkForInstance.class);
    protected final UploadDockerImages uploadDockerImages = mock(UploadDockerImages.class);
    protected TablePropertiesStore tablePropertiesStore;
    protected TableIndex tableIndex;

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
        LocalStackTestBase.createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3, dynamoDB);
        tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDB);
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        tablePropertiesStore.save(tableProperties);
    }
}
