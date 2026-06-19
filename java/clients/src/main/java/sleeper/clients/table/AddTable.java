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

package sleeper.clients.table;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.statestore.InitialiseStateStoreFromSplitPoints;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class AddTable {
    private final TableProperties tableProperties;
    private final TablePropertiesStore tablePropertiesStore;
    private final StateStoreProvider stateStoreProvider;

    public AddTable(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            TablePropertiesStore tablePropertiesStore, StateStoreProvider stateStoreProvider) {
        this.tableProperties = tableProperties;
        this.tablePropertiesStore = tablePropertiesStore;
        this.stateStoreProvider = stateStoreProvider;
    }

    public void run() throws IOException {
        tableProperties.validate();
        tablePropertiesStore.createTable(tableProperties);
        new InitialiseStateStoreFromSplitPoints(stateStoreProvider, tableProperties).run();
    }

    public static void main(String[] rawArgs) throws IOException {
        CommandLineUsage usage = CommandLineUsage.builder()
                .positionalArguments(List.of("instance-id"))
                .options(List.of(
                        CommandOption.longOption("table-name"),
                        CommandOption.longOption("schema"),
                        CommandOption.longOption("table-properties"),
                        CommandOption.longOption("config-dir")))
                .helpSummary("" +
                        "Adds a new table to an existing Sleeper instance.\n" +
                        "\n" +
                        "--table-name <name>\n" +
                        "Name of the new table. May also be set in --table-properties or --config-dir. " +
                        "If --table-name is provided alongside --table-properties or --config-dir, it overrides the name in the file.\n" +
                        "\n" +
                        "--schema <file>\n" +
                        "Path to the schema JSON file.\n" +
                        "\n" +
                        "--table-properties <file>\n" +
                        "Optional path to a table properties file. If not set, default table properties will be used.\n" +
                        "\n" +
                        "--config-dir <dir>\n" +
                        "Path to a directory containing schema.json and table.properties. " +
                        "Can be combined with --schema or --table-properties to override the corresponding file " +
                        "from the directory, but not both at the same time.")
                .build();
        Arguments args = CommandArguments.parseAndValidateOrExit(usage, rawArgs, arguments -> {
            return new Arguments(
                    arguments.getString("instance-id"),
                    arguments.getOptionalString("table-name").orElse(null),
                    getPath(arguments.getOptionalString("schema")),
                    getPath(arguments.getOptionalString("table-properties")),
                    getPath(arguments.getOptionalString("config-dir")));
        });

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();

            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, args.instanceId());

            Path schemaFile = args.resolveSchemaFile();

            TableProperties tableProperties = createTableProperties(instanceProperties, args);
            if (tableProperties.get(TABLE_NAME) == null || tableProperties.get(TABLE_NAME).isBlank()) {
                System.out.println(usage.createUsageMessage());
                System.out.println("Table name was not found. Provide --table-name, or set it in --table-properties or --config-dir.");
                System.exit(1);
            }
            tableProperties.setSchema(Schema.load(schemaFile));

            TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
            new AddTable(instanceProperties, tableProperties, tablePropertiesStore, stateStoreProvider).run();
        }
    }

    static TableProperties createTableProperties(InstanceProperties instanceProperties, Arguments args) throws IOException {
        TableProperties tableProperties;
        if (args.tablePropertiesFile() != null) {
            tableProperties = new TableProperties(instanceProperties, loadProperties(args.tablePropertiesFile()));
        } else if (args.configDir() != null) {
            tableProperties = new TableProperties(instanceProperties, loadProperties(args.configDir().resolve("table.properties")));
        } else {
            tableProperties = new TableProperties(instanceProperties);
        }
        if (args.tableName() != null) {
            tableProperties.set(TABLE_NAME, args.tableName());
        }
        return tableProperties;
    }

    private static Path getPath(Optional<String> pathString) {
        if (pathString.isPresent()) {
            return Path.of(pathString.get());
        }
        return null;
    }

    public record Arguments(
            String instanceId,
            String tableName,
            Path schemaFile,
            Path tablePropertiesFile,
            Path configDir) {

        public Arguments {
            if (instanceId == null) {
                throw new CommandArgumentsException("instance-id must not be null");
            }
            if (tableName == null && tablePropertiesFile == null && configDir == null) {
                throw new CommandArgumentsException("A table name is required. Provide --table-name, or set it in --table-properties or --config-dir.");
            }

            if (schemaFile == null && configDir == null) {
                throw new CommandArgumentsException("Either --schema or --config-dir must be provided");
            }

            if (schemaFile != null && tablePropertiesFile != null && configDir != null) {
                throw new CommandArgumentsException("Cannot specify --schema, --table-properties, and --config-dir together");
            }
        }

        public Path resolveSchemaFile() {
            return schemaFile != null ? schemaFile : configDir.resolve("schema.json");
        }
    }
}
