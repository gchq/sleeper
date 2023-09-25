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
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.InitialiseStateStore;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A utility class to reinitialise a table. It deletes all the data in the table
 * and all the information in the state store. Then the state store for the table
 * is reinitialised using the split points in the provided file.
 */
public class ReinitialiseTableFromSplitPoints extends ReinitialiseTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReinitialiseTableFromSplitPoints.class);
    private final boolean splitPointStringsBase64Encoded;
    private final String splitPointsFileLocation;

    public ReinitialiseTableFromSplitPoints(
            AmazonS3 s3Client,
            AmazonDynamoDB dynamoDBClient,
            String instanceId,
            String tableName,
            String splitPointsFileLocation,
            boolean splitPointStringsBase64Encoded) {
        super(s3Client, dynamoDBClient, instanceId, tableName, true);
        this.splitPointStringsBase64Encoded = splitPointStringsBase64Encoded;
        this.splitPointsFileLocation = splitPointsFileLocation;
    }

    @Override
    protected void initialiseStateStore(TableProperties tableProperties, StateStore stateStore) throws IOException, StateStoreException {
        List<Object> splitPoints = calculateSplitPoints(tableProperties);
        InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(tableProperties, stateStore, splitPoints).run();
    }

    private List<Object> calculateSplitPoints(TableProperties tableProperties) throws IOException {
        List<Object> splitPoints = null;
        if (splitPointsFileLocation != null && !splitPointsFileLocation.isEmpty()) {
            splitPoints = new ArrayList<>();

            PrimitiveType rowKey1Type = tableProperties.getSchema().getRowKeyTypes().get(0);
            List<String> lines = new ArrayList<>();
            try (InputStream inputStream = new FileInputStream(splitPointsFileLocation);
                 Reader tempReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(tempReader)) {
                String lineFromFile = reader.readLine();
                while (null != lineFromFile) {
                    lines.add(lineFromFile);
                    lineFromFile = reader.readLine();
                }
            }
            for (String line : lines) {
                if (rowKey1Type instanceof IntType) {
                    splitPoints.add(Integer.parseInt(line));
                } else if (rowKey1Type instanceof LongType) {
                    splitPoints.add(Long.parseLong(line));
                } else if (rowKey1Type instanceof StringType) {
                    if (splitPointStringsBase64Encoded) {
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
            LOGGER.info("Read {} split points from file", splitPoints.size());
        }
        return splitPoints;
    }

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> " +
                    "<split-points-file-location> " +
                    "<optional-split-points-file-is-base64-encoded-true-or-false>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        String splitPointsFile = args[2];
        boolean splitPointsFileBase64Encoded = args.length != 3 && Boolean.parseBoolean(args[3]);

        System.out.println("If you continue all data will be deleted in the table.");
        System.out.println("The metadata about the partitions will be deleted and replaced "
                + "by new partitions derived from the provided split points.");
        String choice = System.console().readLine("Are you sure you want to delete the data and " +
                "reinitialise this table?\nPlease enter Y or N: ");
        if (!choice.equalsIgnoreCase("y")) {
            System.exit(0);
        }
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        try {
            ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(amazonS3, dynamoDBClient, instanceId, tableName,
                    splitPointsFile, splitPointsFileBase64Encoded);
            reinitialiseTable.run();
            LOGGER.info("Table reinitialised successfully");
        } catch (RuntimeException | IOException | StateStoreException e) {
            LOGGER.error("\nAn Error occurred while trying to reinitialise the table. " +
                    "The error message is as follows:\n\n" + e.getMessage()
                    + "\n\nCause:" + e.getCause());
        }
        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
