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
                .positionalArguments(List.of("instance-id", "table-name"))
                .options(List.of(
                        CommandOption.longOption("schema"),
                        CommandOption.longOption("table-properties"),
                        CommandOption.longOption("config-dir")))
                .helpSummary("" +
                        "Adds a new table to an existing Sleeper instance.\n" +
                        "\n" +
                        "--schema <file>\n" +
                        "Path to the schema JSON file.\n" +
                        "\n" +
                        "--table-properties <file>\n" +
                        "Optional path to a table properties file. If not set, default table properties will be used.\n" +
                        "\n" +
                        "--config-dir <dir>\n" +
                        "Path to a directory containing schema.json and table.properties. " +
                        "Cannot be combined with --schema or --table-properties.")
                .build();
        Arguments args = CommandArguments.parseAndValidateOrExit(usage, rawArgs, arguments -> {
            Optional<Path> schemaFile = arguments.getOptionalString("schema").map(Path::of);
            Optional<Path> configDir = arguments.getOptionalString("config-dir").map(Path::of);
            if (schemaFile.isEmpty() && configDir.isEmpty()) {
                throw new CommandArgumentsException("Either --schema or --config-dir must be provided");
            }
            if (schemaFile.isPresent() && configDir.isPresent()) {
                throw new CommandArgumentsException("Cannot specify both --schema and --config-dir");
            }
            return new Arguments(
                    arguments.getString("instance-id"),
                    arguments.getString("table-name"),
                    schemaFile,
                    arguments.getOptionalString("table-properties").map(Path::of),
                    configDir);
        });

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();

            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, args.instanceId());

            Path schemaFile = args.schemaFile().orElseGet(() -> args.configDir().get().resolve("schema.json"));
            Optional<Path> tablePropertiesFile = args.tablePropertiesFile()
                    .or(() -> args.configDir().map(dir -> dir.resolve("table.properties")));

            TableProperties tableProperties = tablePropertiesFile.isPresent()
                    ? new TableProperties(instanceProperties, loadProperties(tablePropertiesFile.get()))
                    : new TableProperties(instanceProperties);
            tableProperties.set(TABLE_NAME, args.tableName());
            tableProperties.setSchema(Schema.load(schemaFile));

            TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
            new AddTable(instanceProperties, tableProperties, tablePropertiesStore, stateStoreProvider).run();
        }
    }

    public record Arguments(
            String instanceId,
            String tableName,
            Optional<Path> schemaFile,
            Optional<Path> tablePropertiesFile,
            Optional<Path> configDir) {
    }
}
