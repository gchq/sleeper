/*
 * Copyright 2022 Crown Copyright
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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;

/**
 * Initialises a {@link StateStore}. If a file of split points is
 * provided then these are used to create the initial {@link Partition}s.
 * Each line of the file should contain a single point which is a split in
 * the first dimension of the row key. (Only splitting by the first dimension
 * is supported.) If a file isn't provided then a single root {@link Partition}
 * is created.
 */
public class InitialiseStateStore {
    private final Schema schema;
    private final StateStore stateStore;
    private final List<Object> splitPoints;

    private static final Logger LOGGER = LoggerFactory.getLogger(InitialiseStateStore.class);

    public InitialiseStateStore(TableProperties tableProperties,
                                StateStore stateStore,
                                List<Object> splitPoints) {
        this.schema = tableProperties.getSchema();
        this.stateStore = stateStore;
        this.splitPoints = splitPoints;
    }

    public InitialiseStateStore(Schema schema,
                                StateStore stateStore,
                                List<Object> splitPoints) {
        this.schema = schema;
        this.stateStore = stateStore;
        this.splitPoints = splitPoints;
    }

    public void run() throws StateStoreException {
        // Validate that this appears to be an empty database
        List<Partition> partitions = stateStore.getAllPartitions();
        if (!partitions.isEmpty()) {
            LOGGER.error("This should only be run on a database on which no data has been ingested - this instance has " + partitions.size() + " partitions");
            return;
        }
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        if (!activeFiles.isEmpty()) {
            LOGGER.error("This should only be run on a database on which no data has been ingested - this instance has " + activeFiles.size() + " active files");
            return;
        }
        LOGGER.info("Database appears to be empty");

        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);
        List<Partition> initialPartitions = partitionsFromSplitPoints.construct();

        stateStore.initialise(initialPartitions);
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

        TableProperties tableProperties = new TablePropertiesProvider(s3Client, instanceProperties).getTableProperties(args[1]);

        StateStore stateStore;
        if (tableProperties.get(STATESTORE_CLASSNAME).equals("sleeper.statestore.s3.S3StateStore")) {
            System.out.println("S3 State Store detected");
            Configuration conf = new Configuration();
            conf.set("fs.s3a.aws.credentials.provider", DefaultAWSCredentialsProviderChain.class.getName());
            stateStore = new S3StateStore(instanceProperties, tableProperties, dynamoDBClient, conf);
        } else {
            System.out.println("Dynamo DB State Store detected");
            stateStore = new DynamoDBStateStore(tableProperties, dynamoDBClient);
        }

        List<Object> splitPoints = null;
        if (args.length > 2) {
            splitPoints = new ArrayList<>();
            String splitPointsFile = args[2];
            boolean stringsBase64Encoded = 4 == args.length && Boolean.parseBoolean(args[2]);

            PrimitiveType rowKey1Type = tableProperties.getSchema().getRowKeyTypes().get(0);
            BufferedReader reader = new BufferedReader(new FileReader(splitPointsFile));
            List<String> lines = new ArrayList<>();
            String lineFromFile = reader.readLine();
            while (null != lineFromFile) {
                lines.add(lineFromFile);
                lineFromFile = reader.readLine();
            }
            reader.close();

            for (String line : lines) {
                if (rowKey1Type instanceof IntType) {
                    splitPoints.add(Integer.parseInt(line));
                } else if (rowKey1Type instanceof LongType) {
                    splitPoints.add(Long.parseLong(line));
                } else if (rowKey1Type instanceof StringType) {
                    if (stringsBase64Encoded) {
                        byte[] encodedString = Base64.decodeBase64(line);
                        splitPoints.add(new String(encodedString, StandardCharsets.UTF_8));
                    } else {
                        splitPoints.add(line);
                    }
                } else if (rowKey1Type instanceof ByteArrayType) {
                    splitPoints.add(Base64.decodeBase64(line));
                } else {
                    throw new RuntimeException("Unknown key type " + rowKey1Type);
                }
            }
            LOGGER.info("Read " + splitPoints.size() + " split points from file");
        }

        new InitialiseStateStore(tableProperties, stateStore, splitPoints).run();

        dynamoDBClient.shutdown();
        s3Client.shutdown();
    }
}
