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
package sleeper.systemtest.datageneration;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.api.role.AssumeSleeperRole;
import sleeper.clients.api.role.AssumeSleeperRoleAwsSdk;
import sleeper.clients.api.role.AssumeSleeperRoleHadoop;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

public class InstanceIngestSession implements AutoCloseable {
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final SqsClient sqsClient;
    private final S3AsyncClient s3AsyncClient;
    private final Configuration hadoopConfiguration;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final TableProperties tableProperties;
    private final StateStoreProvider stateStoreProvider;
    private final String localDir;

    private InstanceIngestSession(InstanceProperties instanceProperties, String tableName,
            S3Client s3Client, DynamoDbClient dynamoDbClient, SqsClient sqsClient, S3AsyncClient s3AsyncClient, Configuration hadoopConfiguration,
            String localDir) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.sqsClient = sqsClient;
        this.s3AsyncClient = s3AsyncClient;
        this.hadoopConfiguration = hadoopConfiguration;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient);
        this.tableProperties = tablePropertiesProvider.getByName(tableName);
        this.stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDbClient);
        this.localDir = localDir;
    }

    public static InstanceIngestSession direct(StsClient stsClient, InstanceProperties instanceProperties, String tableName, String localDir) {
        AssumeSleeperRole assumeRole = AssumeSleeperRole.directIngest(instanceProperties);
        return assumeRole(assumeRole, stsClient, instanceProperties, tableName, localDir);
    }

    public static InstanceIngestSession byQueue(StsClient stsClient, InstanceProperties instanceProperties, String tableName, String localDir) {
        AssumeSleeperRole assumeRole = AssumeSleeperRole.ingestByQueue(instanceProperties);
        return assumeRole(assumeRole, stsClient, instanceProperties, tableName, localDir);
    }

    private static InstanceIngestSession assumeRole(
            AssumeSleeperRole assumeRole, StsClient stsClient,
            InstanceProperties instanceProperties, String tableName, String localDir) {
        AssumeSleeperRoleAwsSdk aws = assumeRole.forAwsSdk(stsClient);
        AssumeSleeperRoleHadoop hadoop = assumeRole.forHadoop();
        S3Client s3Client = aws.buildClient(S3Client.builder());
        DynamoDbClient dynamoDbClient = aws.buildClient(DynamoDbClient.builder());
        SqsClient sqsClient = aws.buildClient(SqsClient.builder());
        S3AsyncClient s3AsyncClient = aws.buildClient(S3AsyncClient.builder());
        Configuration hadoopConfiguration = hadoop.setS3ACredentials(HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        return new InstanceIngestSession(instanceProperties, tableName, s3Client, dynamoDbClient, sqsClient, s3AsyncClient, hadoopConfiguration, localDir);
    }

    public S3Client s3() {
        return s3Client;
    }

    public S3AsyncClient s3Async() {
        return s3AsyncClient;
    }

    public SqsClient sqs() {
        return sqsClient;
    }

    public Configuration hadoopConfiguration() {
        return hadoopConfiguration;
    }

    public InstanceProperties instanceProperties() {
        return instanceProperties;
    }

    public TableProperties tableProperties() {
        return tableProperties;
    }

    public TablePropertiesProvider tablePropertiesProvider() {
        return tablePropertiesProvider;
    }

    public StateStoreProvider stateStoreProvider() {
        return stateStoreProvider;
    }

    public String localDir() {
        return localDir;
    }

    @Override
    public void close() {
        s3Client.close();
        dynamoDbClient.close();
        sqsClient.close();
        s3AsyncClient.close();
    }
}
