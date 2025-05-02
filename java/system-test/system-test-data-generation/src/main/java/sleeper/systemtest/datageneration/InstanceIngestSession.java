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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.util.AssumeSleeperRole;
import sleeper.clients.util.AssumeSleeperRoleHadoop;
import sleeper.clients.util.AssumeSleeperRoleV1;
import sleeper.clients.util.AssumeSleeperRoleV2;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

public class InstanceIngestSession implements AutoCloseable {
    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamo;
    private final AmazonSQS sqs;
    private final S3AsyncClient s3Async;
    private final Configuration hadoopConfiguration;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final TableProperties tableProperties;
    private final StateStoreProvider stateStoreProvider;
    private final String localDir;

    private InstanceIngestSession(InstanceProperties instanceProperties, String tableName,
            AmazonS3 s3, AmazonDynamoDB dynamo, AmazonSQS sqs, S3AsyncClient s3Async, Configuration hadoopConfiguration,
            String localDir) {
        this.s3 = s3;
        this.dynamo = dynamo;
        this.sqs = sqs;
        this.s3Async = s3Async;
        this.hadoopConfiguration = hadoopConfiguration;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3, dynamo);
        this.tableProperties = tablePropertiesProvider.getByName(tableName);
        this.stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3, dynamo, hadoopConfiguration);
        this.localDir = localDir;
    }

    public static InstanceIngestSession direct(AWSSecurityTokenService stsClientV1, StsClient stsClientV2, InstanceProperties instanceProperties, String tableName, String localDir) {
        AssumeSleeperRole assumeRole = AssumeSleeperRole.directIngest(instanceProperties);
        return assumeRole(assumeRole, stsClientV1, stsClientV2, instanceProperties, tableName, localDir);
    }

    public static InstanceIngestSession byQueue(AWSSecurityTokenService stsClientV1, StsClient stsClientV2, InstanceProperties instanceProperties, String tableName, String localDir) {
        AssumeSleeperRole assumeRole = AssumeSleeperRole.ingestByQueue(instanceProperties);
        return assumeRole(assumeRole, stsClientV1, stsClientV2, instanceProperties, tableName, localDir);
    }

    private static InstanceIngestSession assumeRole(
            AssumeSleeperRole assumeRole, AWSSecurityTokenService stsClientV1, StsClient stsClientV2,
            InstanceProperties instanceProperties, String tableName, String localDir) {
        AssumeSleeperRoleV1 roleV1 = assumeRole.forAwsV1(stsClientV1);
        AssumeSleeperRoleV2 roleV2 = assumeRole.forAwsV2(stsClientV2);
        AssumeSleeperRoleHadoop roleHadoop = assumeRole.forHadoop();
        AmazonS3 s3 = roleV1.buildClient(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamo = roleV1.buildClient(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqs = roleV1.buildClient(AmazonSQSClientBuilder.standard());
        S3AsyncClient s3Async = roleV2.buildClient(S3AsyncClient.builder());
        Configuration hadoopConfiguration = roleHadoop.setS3ACredentials(HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        return new InstanceIngestSession(instanceProperties, tableName, s3, dynamo, sqs, s3Async, hadoopConfiguration, localDir);
    }

    public AmazonS3 s3() {
        return s3;
    }

    public S3AsyncClient s3Async() {
        return s3Async;
    }

    public AmazonSQS sqs() {
        return sqs;
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
        s3.shutdown();
        dynamo.shutdown();
        sqs.shutdown();
        s3Async.close();
    }
}
