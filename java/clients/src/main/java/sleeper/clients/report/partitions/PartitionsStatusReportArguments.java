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

package sleeper.clients.report.partitions;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.report.PartitionsStatusReport;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import java.io.PrintStream;
import java.util.function.Function;

/**
 * Command line arguments to create a report on the status of partitions in a Sleeper table.
 */
public class PartitionsStatusReportArguments {
    private final String instanceId;
    private final String tableName;
    private final Function<PrintStream, PartitionsStatusReporter> reporter;

    private PartitionsStatusReportArguments(
            String instanceId, String tableName, Function<PrintStream, PartitionsStatusReporter> reporter) {
        this.instanceId = instanceId;
        this.tableName = tableName;
        this.reporter = reporter;
    }

    /**
     * Prints the usage message to explain what arguments can be given.
     *
     * @param out the output to write to
     */
    public static void printUsage(PrintStream out) {
        out.println("Usage: <instance-id> <table-name>");
    }

    /**
     * Reads command line arguments to create a report on the status of partitions in a Sleeper table.
     *
     * @param  args                     the arguments
     * @return                          the parsed arguments
     * @throws IllegalArgumentException if the arguments could not be read
     */
    public static PartitionsStatusReportArguments fromArgs(String... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        return new PartitionsStatusReportArguments(args[0], args[1], PartitionsStatusReporter::new);
    }

    /**
     * Retrieves the state of the Sleeper table and writes the report.
     *
     * @param s3Client     an S3 AWS SDK client
     * @param dynamoClient a DynamoDB AWS SDK client
     * @param out          the output to write to
     */
    public void runReport(S3Client s3Client, DynamoDbClient dynamoClient, PrintStream out) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
        StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);

        new PartitionsStatusReport(stateStore, tableProperties, reporter.apply(out)).run();
    }
}
