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

import sleeper.clients.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.report.compaction.task.CompactionTaskStatusReportArguments;
import sleeper.clients.report.compaction.task.CompactionTaskStatusReporter;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of compaction tasks. A compaction task is a process that starts up, runs some number of
 * compaction jobs, then terminates. This takes a {@link CompactionTaskQuery} and outputs information about the tasks
 * matching that query.
 */
public class CompactionTaskStatusReport {

    private final CompactionTaskTracker tracker;
    private final CompactionTaskStatusReporter reporter;
    private final CompactionTaskQuery query;

    public CompactionTaskStatusReport(
            CompactionTaskTracker tracker,
            CompactionTaskStatusReporter reporter,
            CompactionTaskQuery query) {
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
        CompactionTaskStatusReportArguments arguments;
        try {
            arguments = CompactionTaskStatusReportArguments.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            CompactionTaskStatusReportArguments.printUsage(System.err);
            System.exit(1);
            return;
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, arguments.getInstanceId());
            CompactionTaskTracker tracker = CompactionTaskTrackerFactory.getTracker(dynamoClient, instanceProperties);
            new CompactionTaskStatusReport(tracker, arguments.getReporter(), arguments.getQuery()).run();
        }
    }
}
