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
package sleeper.clients.report;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.report.ingest.task.IngestTaskQuery;
import sleeper.clients.report.ingest.task.IngestTaskStatusReportArguments;
import sleeper.clients.report.ingest.task.IngestTaskStatusReporter;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.ingest.tracker.task.IngestTaskTrackerFactory;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of ingest tasks. An ingest task is a process that starts up, runs some number of
 * ingest jobs, then terminates. Note that this does not include bulk import. This takes an {@link IngestTaskQuery}
 * and outputs information about the tasks matching that query.
 */
public class IngestTaskStatusReport {
    private final IngestTaskTracker tracker;

    private final IngestTaskStatusReporter reporter;
    private final IngestTaskQuery query;

    public IngestTaskStatusReport(
            IngestTaskTracker tracker,
            IngestTaskStatusReporter reporter,
            IngestTaskQuery query) {
        this.tracker = tracker;
        this.reporter = reporter;
        this.query = query;
    }

    /**
     * Creates a report.
     */
    public void run() {
        reporter.report(query, query.run(tracker));
    }

    public static void main(String[] args) {
        IngestTaskStatusReportArguments arguments;
        try {
            arguments = IngestTaskStatusReportArguments.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            IngestTaskStatusReportArguments.printUsage(System.err);
            System.exit(1);
            return;
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, arguments.getInstanceId());
            IngestTaskTracker tracker = IngestTaskTrackerFactory.getTracker(dynamoClient, instanceProperties);
            new IngestTaskStatusReport(tracker, arguments.getReporter(), arguments.getQuery()).run();
        }
    }
}
