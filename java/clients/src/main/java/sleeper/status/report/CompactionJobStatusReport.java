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
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.ConsoleInput;
import sleeper.status.report.compaction.job.CompactionJobStatusReportArguments;
import sleeper.status.report.compaction.job.CompactionJobStatusReporter;
import sleeper.status.report.query.JobQuery;

import java.io.IOException;
import java.time.Clock;

public class CompactionJobStatusReport {
    private final CompactionJobStatusReportArguments arguments;
    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobStatusStore compactionJobStatusStore;

    public CompactionJobStatusReport(
            CompactionJobStatusStore compactionJobStatusStore,
            CompactionJobStatusReportArguments arguments) {
        this.arguments = arguments;
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.compactionJobStatusReporter = arguments.getReporter();
    }

    public void run() {
        JobQuery query = arguments.buildQuery(Clock.systemUTC(),
                new ConsoleInput(System.console()));
        if (query == null) {
            return;
        }
        compactionJobStatusReporter.report(
                query.forCompaction().run(compactionJobStatusStore),
                arguments.getQueryType());
    }

    public static void main(String[] args) throws IOException {
        CompactionJobStatusReportArguments arguments;
        try {
            arguments = CompactionJobStatusReportArguments.from(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            CompactionJobStatusReportArguments.printUsage(System.err);
            System.exit(1);
            return;
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, arguments.getInstanceId());

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        CompactionJobStatusStore statusStore = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);
        new CompactionJobStatusReport(statusStore, arguments).run();
    }
}
