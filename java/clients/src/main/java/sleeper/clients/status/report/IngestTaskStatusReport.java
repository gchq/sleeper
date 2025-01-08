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

import sleeper.clients.status.report.ingest.task.IngestTaskQuery;
import sleeper.clients.status.report.ingest.task.IngestTaskStatusReportArguments;
import sleeper.clients.status.report.ingest.task.IngestTaskStatusReporter;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.ingest.status.store.task.IngestTaskTrackerFactory;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, arguments.getInstanceId());
            IngestTaskTracker tracker = IngestTaskTrackerFactory.getTracker(dynamoDBClient, instanceProperties);
            new IngestTaskStatusReport(tracker, arguments.getReporter(), arguments.getQuery()).run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
