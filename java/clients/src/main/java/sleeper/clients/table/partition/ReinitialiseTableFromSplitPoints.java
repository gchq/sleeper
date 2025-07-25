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
package sleeper.clients.table.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.table.ReinitialiseTable;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.local.ReadSplitPoints.readSplitPoints;

/**
 * A utility class to reinitialise a table. It deletes all the data in the table
 * and all the information in the state store. Then the state store for the table
 * is reinitialised using the split points in the provided file.
 */
public class ReinitialiseTableFromSplitPoints {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReinitialiseTableFromSplitPoints.class);
    private final ReinitialiseTable reinitialiseTable;
    private final boolean splitPointStringsBase64Encoded;
    private final String splitPointsFileLocation;

    public ReinitialiseTableFromSplitPoints(
            S3Client s3Client,
            DynamoDbClient dynamoClient,
            String instanceId,
            String tableName,
            String splitPointsFileLocation,
            boolean splitPointStringsBase64Encoded) {
        this.reinitialiseTable = new ReinitialiseTable(s3Client, dynamoClient, instanceId, tableName, true);
        this.splitPointStringsBase64Encoded = splitPointStringsBase64Encoded;
        this.splitPointsFileLocation = splitPointsFileLocation;
    }

    public void run() {
        reinitialiseTable.run(tableProperties -> new InitialisePartitionsTransaction(readPartitions(tableProperties)));
    }

    private List<Partition> readPartitions(TableProperties tableProperties) {
        try {
            List<Object> splitPoints = readSplitPoints(tableProperties, splitPointsFileLocation, splitPointStringsBase64Encoded);
            return new PartitionsFromSplitPoints(tableProperties.getSchema(), splitPoints).construct();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        if (!"y".equalsIgnoreCase(choice)) {
            System.exit(0);
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            ReinitialiseTableFromSplitPoints reinitialiseTable = new ReinitialiseTableFromSplitPoints(
                    s3Client, dynamoClient, instanceId, tableName,
                    splitPointsFile, splitPointsFileBase64Encoded);
            reinitialiseTable.run();
            LOGGER.info("Table reinitialised successfully");
        }
    }
}
