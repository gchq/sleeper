/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import sleeper.ClientUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.status.report.partitions.PartitionsQuery;
import sleeper.status.report.partitions.PartitionsStatusReportArguments;
import sleeper.status.report.partitions.PartitionsStatusReporter;
import sleeper.status.report.partitions.StandardPartitionsStatusReporter;

import java.io.IOException;

/**
 * A utility class to report information about the partitions in the system and
 * their status.
 */
public class PartitionsStatusReport {
    private final StateStore store;
    private final PartitionsStatusReporter reporter;
    private final PartitionsQuery query;

    public PartitionsStatusReport(StateStore store) {
        this(store, new StandardPartitionsStatusReporter(System.out), PartitionsQuery.ALL);
    }

    public PartitionsStatusReport(StateStore store, PartitionsStatusReporter reporter,
                                  PartitionsQuery query) {
        this.store = store;
        this.reporter = reporter;
        this.query = query;
    }

    public void run() throws StateStoreException {
        reporter.report(query, query.run(store));
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        PartitionsStatusReportArguments arguments;
        try {
            arguments = PartitionsStatusReportArguments.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            PartitionsStatusReportArguments.printUsage(System.err);
            System.exit(1);
            return;
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, arguments.getInstanceId());

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(amazonS3, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, new Configuration());
        StateStore stateStore = stateStoreProvider.getStateStore(arguments.getTableName(), tablePropertiesProvider);

        PartitionsStatusReport statusReport = new PartitionsStatusReport(stateStore, arguments.getReporter(), arguments.getQuery());
        statusReport.run();

        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
