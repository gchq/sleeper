/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.bulkexport;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.clients.api.BulkExportQuerySender;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_S3_BUCKET;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Submits bulk export jobs from the command line.
 */
public class BulkExportClient {
    private final InstanceProperties instanceProperties;
    private final TableIndex tableIndex;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final BulkExportQuerySender sender;
    private final ConsoleInput in;
    private final ConsoleOutput out;
    private final Supplier<String> exportIdSupplier;

    public BulkExportClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            BulkExportQuerySender sender, ConsoleInput in, ConsoleOutput out) {
        this(instanceProperties, tableIndex, tablePropertiesProvider, sender, in, out, () -> UUID.randomUUID().toString());
    }

    public BulkExportClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            BulkExportQuerySender sender, ConsoleInput in, ConsoleOutput out, Supplier<String> exportIdSupplier) {
        this.instanceProperties = instanceProperties;
        this.tableIndex = tableIndex;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.sender = sender;
        this.in = in;
        this.out = out;
        this.exportIdSupplier = exportIdSupplier;
    }

    /**
     * Prompts for a table to export, then submits bulk exports of it until the user chooses to stop.
     */
    public void run() {
        if (instanceProperties.get(BULK_EXPORT_QUEUE_URL) == null) {
            throw new IllegalArgumentException("Use of this client requires the bulk export stack to have been deployed as part of your Sleeper instance!");
        }
        String tableName = promptTableName();
        if (tableName == null) {
            return;
        }
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
        out.println("The table has the schema " + tableProperties.getSchema());
        do {
            submitExport(tableProperties);
        } while ("y".equalsIgnoreCase(in.promptLine("Submit another bulk export? (y/n) ").strip()));
    }

    private void submitExport(TableProperties tableProperties) {
        BulkExportQuery query = BulkExportQuery.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .exportId(exportIdSupplier.get())
                .sqlQuery(promptSqlQuery(tableProperties))
                .build();
        sender.sendQueryToBulkExport(query);
        out.println("Submitted bulk export with id " + query.getExportId());
        out.println("Results will be written to s3://" + instanceProperties.get(BULK_EXPORT_S3_BUCKET)
                + "/" + tableProperties.get(TABLE_ID) + "/" + query.getExportId() + "/");
    }

    private String promptTableName() {
        List<String> tables = tableIndex.streamAllTables()
                .map(TableStatus::getTableName)
                .toList();
        if (tables.isEmpty()) {
            out.println("There are no tables. Please create one and add data before running this class.");
            return null;
        }
        if (tables.size() == 1) {
            String tableName = tables.get(0);
            out.println("Exporting table " + tableName);
            return tableName;
        }
        while (true) {
            out.println("The system contains the following tables:");
            tables.forEach(out::println);
            String tableName = in.promptLine("Which table do you wish to export? ");
            if (tables.contains(tableName)) {
                return tableName;
            }
            out.println("Invalid table, try again");
        }
    }

    private String promptSqlQuery(TableProperties tableProperties) {
        if (tableProperties.getEnumValue(DATA_ENGINE, DataEngine.class) == DataEngine.JAVA) {
            out.println("The table uses the Java data engine, so an SQL statement cannot be applied to the export. "
                    + "This requires the DataFusion data engine.");
            return null;
        }
        out.println("Enter an optional SQL statement to execute on the exported data. Table name is \"query_results\".");
        String sqlQuery = in.promptLine("Enter SQL statement (blank for none): ");
        if (sqlQuery.strip().isEmpty()) {
            return null;
        } else {
            return sqlQuery;
        }
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        String instanceId = args[0];

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                SqsClient sqsClient = SqsClient.create();
                StsClient stsClient = StsClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
            TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, tableIndex, s3Client);
            new BulkExportClient(
                    instanceProperties, tableIndex, tablePropertiesProvider,
                    BulkExportQuerySender.toSqs(instanceProperties, sqsClient),
                    ConsoleInput.stdIn(), ConsoleOutput.stdOut())
                    .run();
        }
    }
}
