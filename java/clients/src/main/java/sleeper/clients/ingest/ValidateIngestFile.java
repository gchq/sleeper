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
package sleeper.clients.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.parquet.row.ParquetSchemaValidation;
import sleeper.parquet.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Checks a Parquet file's schema against a Sleeper table without submitting an ingest job.
 */
public class ValidateIngestFile {
    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name", "file-path"))
            .helpSummary("Checks a local Parquet file or s3:// path against a table for standard ingest. " +
                    "Reads only the file footer; rows are not scanned and Spark bulk import is not validated.")
            .build();

    private final TablePropertiesStore tables;
    private final Configuration configuration;
    private final ConsoleOutput out;

    public ValidateIngestFile(TablePropertiesStore tables, Configuration configuration, ConsoleOutput out) {
        this.tables = tables;
        this.configuration = configuration;
        this.out = out;
    }

    public static void main(String[] rawArgs) throws IOException {
        CommandArguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs);
        boolean valid;
        try (S3Client s3 = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamo = buildAwsV2Client(DynamoDbClient.builder());
                StsClient sts = buildAwsV2Client(StsClient.builder())) {
            InstanceProperties instance = S3InstanceProperties.loadGivenAccountAndInstanceId(
                    s3, sts.getCallerIdentity().account(), args.getString("instance-id"));
            valid = new ValidateIngestFile(S3TableProperties.createStore(instance, s3, dynamo),
                    HadoopConfigurationProvider.getConfigurationForClient(instance), ConsoleOutput.stdOut()).run(args);
        }
        if (!valid) {
            System.exit(1);
        }
    }

    /**
     * Checks a file using the supplied command line arguments and writes the result to the console.
     *
     * @param  rawArgs     the instance ID, table name and file path
     * @return             true if the file schema is compatible
     * @throws IOException if the file cannot be read
     */
    public boolean run(String... rawArgs) throws IOException {
        return run(CommandArgumentReader.parse(USAGE, rawArgs));
    }

    private boolean run(CommandArguments args) throws IOException {
        String tableName = args.getString("table-name");
        Schema schema = tables.loadByName(tableName).getSchema();
        String file = args.getString("file-path");
        Path path = new Path(file.startsWith("s3://") ? "s3a://" + file.substring(5) : file);
        MessageType fileSchema;
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration))) {
            fileSchema = reader.getFooter().getFileMetaData().getSchema();
        }
        List<String> problems = ParquetSchemaValidation.validate(schema, fileSchema);
        out.println("File: " + file);
        out.println("Table: " + tableName);
        if (problems.isEmpty()) {
            out.println("Schema is compatible with standard ingest.");
        } else {
            out.println("Schema is not compatible with standard ingest:");
            problems.forEach(problem -> out.println("  " + problem));
        }
        out.println("Only the schema was checked. Row values and file data were not scanned.");
        out.println("Spark bulk import is not validated; its schema and null-handling rules may differ.");
        return problems.isEmpty();
    }
}
