/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.deploy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

import sleeper.clients.deploy.DeployNewInstance.StoreFactory;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DeployNewInstanceIT extends LocalStackTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
    }

    @Test
    void shouldDeployInstanceByInstanceProperties() throws IOException, InterruptedException {
        // Given
        SleeperInstanceConfiguration config = SleeperInstanceConfiguration.withNoTables(instanceProperties);

        // When
        deployInstance(config);

        // Then
        assertThat(propertiesStore.streamAllTables()).isEmpty();
    }

    @Test
    void shouldDeployInstanceByConfigDirectoryWithTables() {

    }

    @Test
    void shouldDeployInstanceByConfigDirectoryIgnoringTables() {

    }

    private StateStore stateStore(TableProperties tableProperties) {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    private void deployInstance(SleeperInstanceConfiguration config) throws IOException, InterruptedException {
        String accountName = stsClient.getCallerIdentity().account();
        Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
        PartitionMetadata partitionMetadata = PartitionMetadata.of(region);

        new DeployNewInstance(DeployInstance.fromScriptsDirectory(Path.of("TODO"), accountName, region, partitionMetadata, s3Client, ecrClient),
                StoreFactory.withAwsClients(s3Client, dynamoClient),
                config, SleeperInternalCdkApp.STANDARD, false, false).deploy();
    }
}
