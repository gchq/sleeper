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
package sleeper.statestore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.io.IOException;
import java.util.List;

import static sleeper.configuration.ReadSplitPoints.readSplitPoints;

/**
 * Initialises a {@link StateStore}. If a file of split points is
 * provided then these are used to create the initial {@link Partition}s.
 * Each line of the file should contain a single point which is a split in
 * the first dimension of the row key. (Only splitting by the first dimension
 * is supported.) If a file isn't provided then a single root {@link Partition}
 * is created.
 */
public class InitialiseStateStoreFromSplitPoints {
    private final StateStoreProvider stateStoreProvider;
    private final TableProperties tableProperties;
    private final List<Object> splitPoints;

    public InitialiseStateStoreFromSplitPoints(
            StateStoreProvider stateStoreProvider, TableProperties tableProperties) throws IOException {
        this(stateStoreProvider, tableProperties, readSplitPoints(tableProperties));
    }

    public InitialiseStateStoreFromSplitPoints(
            StateStoreProvider stateStoreProvider, TableProperties tableProperties, List<Object> splitPoints) {
        this.stateStoreProvider = stateStoreProvider;
        this.tableProperties = tableProperties;
        this.splitPoints = splitPoints;
    }

    public void run() {
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        try {
            stateStore.initialise(new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct());
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed to initialise State Store", e);
        }
    }

    public static void main(String[] args) throws StateStoreException, IOException {
        if (2 != args.length && 3 != args.length && 4 != args.length) {
            System.out.println("Usage: <Sleeper S3 Config Bucket> <Table name> <optional split points file> <optional boolean strings base64 encoded>");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, args[0]);

        TableProperties tableProperties = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient).getTableProperties(args[1]);

        List<Object> splitPoints = null;
        if (args.length > 2) {
            String splitPointsFile = args[2];
            boolean stringsBase64Encoded = 4 == args.length && Boolean.parseBoolean(args[2]);
            splitPoints = readSplitPoints(tableProperties, splitPointsFile, stringsBase64Encoded);
        }

        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, new Configuration());

        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties, splitPoints).run();

        dynamoDBClient.shutdown();
        s3Client.shutdown();
    }
}
