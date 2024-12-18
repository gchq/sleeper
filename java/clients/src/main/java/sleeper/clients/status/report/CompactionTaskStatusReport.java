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

import sleeper.clients.status.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.status.report.compaction.task.CompactionTaskStatusReportArguments;
import sleeper.clients.status.report.compaction.task.CompactionTaskStatusReporter;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.task.CompactionTaskStatusStore;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, arguments.getInstanceId());
            CompactionTaskStatusStore statusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
            new CompactionTaskStatusReport(statusStore, arguments.getReporter(), arguments.getQuery()).run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
