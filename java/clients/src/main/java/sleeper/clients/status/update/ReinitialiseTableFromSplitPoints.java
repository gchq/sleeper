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
package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.io.IOException;
import java.util.List;

import static sleeper.configuration.ReadSplitPoints.readSplitPoints;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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
        List<Object> splitPoints = readSplitPoints(tableProperties, splitPointsFileLocation, splitPointStringsBase64Encoded);
        stateStore.initialise(new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct());
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
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        try {
            ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(s3Client, dynamoDBClient, instanceId, tableName,
                    splitPointsFile, splitPointsFileBase64Encoded);
            reinitialiseTable.run();
            LOGGER.info("Table reinitialised successfully");
        } catch (RuntimeException | IOException | StateStoreException e) {
            LOGGER.error("\nAn Error occurred while trying to reinitialise the table. " +
                    "The error message is as follows:\n\n" + e.getMessage()
                    + "\n\nCause:" + e.getCause());
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
