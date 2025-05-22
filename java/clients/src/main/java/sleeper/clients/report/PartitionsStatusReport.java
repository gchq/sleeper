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
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.clients.report.partitions.PartitionsStatusReportArguments;
import sleeper.clients.report.partitions.PartitionsStatusReporter;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.splitter.core.status.PartitionsStatus;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * A utility class to report information about the partitions in the system and
 * their status.
 */
public class PartitionsStatusReport {
    private final StateStore store;
    private final TableProperties tableProperties;
    private final PartitionsStatusReporter reporter;

    public PartitionsStatusReport(StateStore store, TableProperties tableProperties, PartitionsStatusReporter reporter) {
        this.store = store;
        this.tableProperties = tableProperties;
        this.reporter = reporter;
    }

    public void run() {
        reporter.report(PartitionsStatus.from(tableProperties, store));
    }

    public static void main(String[] args) {
        PartitionsStatusReportArguments arguments;
        try {
            arguments = PartitionsStatusReportArguments.fromArgs(args);
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());
            PartitionsStatusReportArguments.printUsage(System.err);
            System.exit(1);
            return;
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            arguments.runReport(s3Client, s3TransferManager, dynamoClient, System.out);
        }
    }
}
