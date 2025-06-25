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
import sleeper.core.partition.PartitionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * A utility class to reinitialise a table. It deletes all the data in the table
 * and all the information in the state store. Then the state store for the table
 * is reinitialised using the partitions in the provided file. This file should
 * have been created using the class {@link ExportPartitions}.
 */
public class ReinitialiseTableFromExportedPartitions {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReinitialiseTableFromExportedPartitions.class);
    private final ReinitialiseTable reinitialiseTable;
    private final String partitionsFile;

    public ReinitialiseTableFromExportedPartitions(
            S3Client s3Client,
            DynamoDbClient dynamoClient,
            String instanceId,
            String tableName,
            String partitionsFile) {
        this.reinitialiseTable = new ReinitialiseTable(s3Client, dynamoClient, instanceId, tableName, true);
        this.partitionsFile = partitionsFile;
    }

    public void run() {
        reinitialiseTable.run(tableProperties -> new InitialisePartitionsTransaction(readPartitions(tableProperties.getSchema())));
    }

    private List<Partition> readPartitions(Schema schema) {
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);
        List<Partition> partitions = new ArrayList<>();
        LOGGER.info("Attempting to read partitions from file {}", partitionsFile);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(partitionsFile), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    partitions.add(partitionSerDe.fromJson(line));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOGGER.info("Read {} partitions from file", partitions.size());

        return partitions;
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <exported-partitions-file-location>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        String exportedPartitionsFile = args[2];

        System.out.println("If you continue all data will be deleted in the table.");
        System.out.println("The metadata about the partitions will be deleted and replaced "
                + "by new partitions derived from the provided partitions file.");
        String choice = System.console().readLine("Are you sure you want to delete the data and " +
                "reinitialise this table?\nPlease enter Y or N: ");
        if (!"y".equalsIgnoreCase(choice)) {
            System.exit(0);
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            ReinitialiseTableFromExportedPartitions reinitialiseTable = new ReinitialiseTableFromExportedPartitions(
                    s3Client, dynamoClient, instanceId, tableName, exportedPartitionsFile);
            reinitialiseTable.run();
            LOGGER.info("Table reinitialised successfully");
        }
    }
}
