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

import sleeper.clients.status.report.ingest.batcher.BatcherQuery;
import sleeper.clients.status.report.ingest.batcher.IngestBatcherReporter;
import sleeper.clients.status.report.ingest.batcher.JsonIngestBatcherReporter;
import sleeper.clients.status.report.ingest.batcher.StandardIngestBatcherReporter;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.TableStatusProvider;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class IngestBatcherReport {
    private static final Map<String, BatcherQuery.Type> QUERY_TYPES = new HashMap<>();

    static {
        QUERY_TYPES.put("-a", BatcherQuery.Type.ALL);
        QUERY_TYPES.put("-p", BatcherQuery.Type.PENDING);
    }

    enum ReporterType {
        JSON,
        STANDARD
    }

    private final IngestBatcherStore batcherStore;
    private final IngestBatcherReporter reporter;
    private final BatcherQuery.Type queryType;
    private final BatcherQuery query;
    private final TableStatusProvider tableProvider;

    public IngestBatcherReport(
            IngestBatcherStore batcherStore, IngestBatcherReporter reporter,
            BatcherQuery.Type queryType, TableStatusProvider tableProvider) {
        this.batcherStore = batcherStore;
        this.reporter = reporter;
        this.query = BatcherQuery.from(queryType, new ConsoleInput(System.console()));
        this.queryType = query.getType();
        this.tableProvider = tableProvider;
    }

    public void run() {
        if (query == null) {
            return;
        }
        reporter.report(query.run(batcherStore), queryType, tableProvider);
    }

    public static void main(String[] args) {
        String instanceId = null;
        ReporterType reporterType = null;
        BatcherQuery.Type queryType = null;
        try {
            if (args.length < 2 || args.length > 3) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            instanceId = args[0];
            reporterType = optionalArgument(args, 1)
                    .map(str -> str.toUpperCase(Locale.ROOT))
                    .map(ReporterType::valueOf)
                    .orElse(ReporterType.STANDARD);
            queryType = optionalArgument(args, 2)
                    .map(IngestBatcherReport::readQueryType)
                    .orElse(BatcherQuery.Type.PROMPT);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            IngestBatcherStore statusStore = new DynamoDBIngestBatcherStore(dynamoDBClient, instanceProperties,
                    new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient));
            IngestBatcherReporter reporter;
            switch (reporterType) {
                case JSON:
                    reporter = new JsonIngestBatcherReporter();
                    break;
                case STANDARD:
                default:
                    reporter = new StandardIngestBatcherReporter();
            }
            new IngestBatcherReport(statusStore, reporter, queryType,
                    new TableStatusProvider(new DynamoDBTableIndex(instanceProperties, dynamoDBClient)))
                    .run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }

    private static BatcherQuery.Type readQueryType(String queryTypeStr) {
        if (!QUERY_TYPES.containsKey(queryTypeStr)) {
            throw new IllegalArgumentException("Invalid query type " + queryTypeStr);
        }
        return QUERY_TYPES.get(queryTypeStr);
    }

    private static void printUsage() {
        System.out.println("" +
                "Usage: <instance-id> <report-type-standard-or-json> <optional-query-type>\n" +
                "Query types are:\n" +
                "-a (All files)\n" +
                "-p (Pending files)");
    }
}
