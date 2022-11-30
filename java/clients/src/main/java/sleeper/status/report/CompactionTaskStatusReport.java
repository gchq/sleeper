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
import sleeper.ClientUtils;
import sleeper.compaction.status.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.status.report.compaction.task.CompactionTaskQuery;
import sleeper.status.report.compaction.task.CompactionTaskStatusReporter;
import sleeper.status.report.compaction.task.StandardCompactionTaskStatusReporter;

import java.io.IOException;
import java.io.PrintStream;

import static sleeper.ClientUtils.optionalArgument;

public class CompactionTaskStatusReport {

    private final CompactionTaskStatusStore store;
    private final CompactionTaskStatusReporter reporter;
    private final CompactionTaskQuery query;

    public CompactionTaskStatusReport(
            CompactionTaskStatusStore store,
            CompactionTaskStatusReporter reporter,
            CompactionTaskQuery query) {
        this.store = store;
        this.reporter = reporter;
        this.query = query;
    }

    public void run() {
        reporter.report(query, query.run(store));
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 3) {
            System.out.println("Wrong number of arguments");
            printUsage(System.out);
            System.exit(1);
            return;
        }
        String instanceId = args[0];
        CompactionTaskStatusReporter reporter;
        CompactionTaskQuery query;
        try {
            reporter = optionalArgument(args, 1)
                    .map(type -> CompactionTaskStatusReporter.from(type, System.out))
                    .orElseGet(() -> new StandardCompactionTaskStatusReporter(System.out));
            query = optionalArgument(args, 2)
                    .map(CompactionTaskQuery::from)
                    .orElse(CompactionTaskQuery.ALL);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage(System.out);
            System.exit(1);
            return;
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, instanceId);

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        CompactionTaskStatusStore statusStore = DynamoDBCompactionTaskStatusStore.from(dynamoDBClient, instanceProperties);
        new CompactionTaskStatusReport(statusStore, reporter, query).run();
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: <instance id> <report_type_standard_or_json> <optional_query_type>\n" +
                "Query types are:\n" +
                "-a (Return all tasks)\n" +
                "-u (Unfinished tasks)");
    }
}
