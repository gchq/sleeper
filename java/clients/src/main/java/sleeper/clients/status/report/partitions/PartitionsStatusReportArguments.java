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

package sleeper.clients.status.report.partitions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.status.report.PartitionsStatusReport;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;

import java.io.PrintStream;
import java.util.function.Function;

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

    public static void printUsage(PrintStream out) {
        out.println("Usage: <instance-id> <table-name>");
    }

    public static PartitionsStatusReportArguments fromArgs(String... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        return new PartitionsStatusReportArguments(args[0], args[1], PartitionsStatusReporter::new);
    }

    public void runReport(AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, PrintStream out) throws StateStoreException {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
        StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, new Configuration());
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);

        new PartitionsStatusReport(stateStore, tableProperties, reporter.apply(out)).run();
    }
}
