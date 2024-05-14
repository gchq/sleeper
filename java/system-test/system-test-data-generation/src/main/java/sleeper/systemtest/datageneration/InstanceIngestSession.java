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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.clients.util.AssumeSleeperRole;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreProvider;

public class InstanceIngestSession implements AutoCloseable {
    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamo;
    private final AmazonSQS sqs;
    private final S3AsyncClient s3Async;
    private final Configuration hadoopConfiguration;
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;

    public InstanceIngestSession(AssumeSleeperRole role, InstanceProperties instanceProperties, String tableName) {
        this.s3 = role.v1Client(AmazonS3ClientBuilder.standard());
        this.dynamo = role.v1Client(AmazonDynamoDBClientBuilder.standard());
        this.sqs = role.v1Client(AmazonSQSClientBuilder.standard());
        this.s3Async = role.v2Client(S3AsyncClient.builder());
        this.hadoopConfiguration = role.setS3ACredentials(HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        this.instanceProperties = instanceProperties;
        this.tableProperties = new TablePropertiesProvider(instanceProperties, s3, dynamo)
                .getByName(tableName);
    }

    public AmazonS3 s3() {
        return s3;
    }

    public S3AsyncClient s3Async() {
        return s3Async;
    }

    public AmazonDynamoDB dynamo() {
        return dynamo;
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

    public StateStoreProvider createStateStoreProvider() {
        return new StateStoreProvider(instanceProperties, s3, dynamo, hadoopConfiguration);
    }

    @Override
    public void close() {
        s3.shutdown();
        dynamo.shutdown();
        sqs.shutdown();
        s3Async.close();
    }
}
