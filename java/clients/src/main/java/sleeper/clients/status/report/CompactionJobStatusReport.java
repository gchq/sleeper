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

package sleeper.clients.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.status.report.compaction.job.CompactionJobStatusReporter;
import sleeper.clients.status.report.compaction.job.JsonCompactionJobStatusReporter;
import sleeper.clients.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.status.report.job.query.JobQueryArgument;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;

import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class CompactionJobStatusReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardCompactionJobStatusReporter());
        REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobTracker compactionJobStatusStore;
    private final JobQuery.Type queryType;
    private final JobQuery query;

    public CompactionJobStatusReport(
            CompactionJobTracker compactionJobStatusStore,
            CompactionJobStatusReporter reporter,
            TableStatus table, JobQuery.Type queryType) {
        this(compactionJobStatusStore, reporter, table, queryType, "");
    }

    public CompactionJobStatusReport(
            CompactionJobTracker compactionJobStatusStore,
            CompactionJobStatusReporter reporter,
            TableStatus table, JobQuery.Type queryType, String queryParameters) {
        this(compactionJobStatusStore, reporter,
                JobQuery.fromParametersOrPrompt(table, queryType, queryParameters,
                        Clock.systemUTC(), new ConsoleInput(System.console())));
    }

    public CompactionJobStatusReport(
            CompactionJobTracker compactionJobStatusStore,
            CompactionJobStatusReporter reporter,
            JobQuery query) {
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.compactionJobStatusReporter = reporter;
        this.query = query;
        this.queryType = query.getType();
    }

    public void run() {
        if (query == null) {
            return;
        }
        compactionJobStatusReporter.report(query.run(compactionJobStatusStore), queryType);
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2 || args.length > 5) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            String instanceId = args[0];
            String tableName = args[1];
            CompactionJobStatusReporter reporter = getReporter(args, 2);
            JobQuery.Type queryType = JobQueryArgument.readTypeArgument(args, 3);
            String queryParameters = optionalArgument(args, 4).orElse(null);

            AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
            AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

            try {
                InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
                DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDBClient);
                TableStatus table = tableIndex.getTableByName(tableName)
                        .orElseThrow(() -> new IllegalArgumentException("Table does not exist: " + tableName));
                CompactionJobTracker statusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
                new CompactionJobStatusReport(statusStore, reporter, table, queryType, queryParameters).run();
            } finally {
                s3Client.shutdown();
                dynamoDBClient.shutdown();
            }
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("Usage: <instance-id> <table-name> <report-type-standard-or-json> <optional-query-type> <optional-query-parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
    }

    private static CompactionJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }
}
