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

package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TablePropertiesStore;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIndex;
import sleeper.statestore.InitialiseStateStoreFromSplitPoints;
import sleeper.statestore.StateStoreProvider;
import sleeper.table.index.dynamodb.DynamoDBTableIndex;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class AddTable {
    private final TableProperties tableProperties;
    private final TablePropertiesStore tablePropertiesStore;
    private final TableIndex tableIndex;
    private final StateStoreProvider stateStoreProvider;

    public AddTable(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties,
                    TableProperties tableProperties) {
        this.tableProperties = tableProperties;
        this.tablePropertiesStore = new S3TablePropertiesStore(instanceProperties, s3Client);
        this.tableIndex = new DynamoDBTableIndex(dynamoDB, instanceProperties);
        this.stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, new Configuration());
    }

    public void run() throws IOException {
        tableIndex.createTable(tableProperties.get(TABLE_NAME));
        tablePropertiesStore.save(tableProperties);
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties).run();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: <instance-id> <table-properties-file> <schema-file>");
            return;
        }

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, args[0]);

        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(Schema.load(Path.of(args[2])));
        tableProperties.load(Path.of(args[1]));

        new AddTable(s3Client, dynamoDBClient, instanceProperties, tableProperties).run();
        dynamoDBClient.shutdown();
        s3Client.shutdown();
    }
}
