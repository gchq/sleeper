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
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.status.report.compactionjob.CompactionJobStatusCollector;
import sleeper.status.report.compactionjob.CompactionJobStatusReportArguments;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class CompactionJobStatusReport {
    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobStatusCollector compactionJobStatusCollector;
    private final SimpleDateFormat dateInputFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    private static final Instant DEFAULT_RANGE_START = Instant.now().minus(4L, ChronoUnit.HOURS);
    private static final Instant DEFAULT_RANGE_END = Instant.now();

    public CompactionJobStatusReport(
            AmazonDynamoDB dynamoDB,
            InstanceProperties instanceProperties,
            CompactionJobStatusReportArguments arguments) {
        CompactionJobStatusStore compactionJobStatusStore = DynamoDBCompactionJobStatusStore.from(dynamoDB, instanceProperties);
        compactionJobStatusCollector = new CompactionJobStatusCollector(compactionJobStatusStore, arguments.getTableName());
        compactionJobStatusReporter = arguments.getReporter();
    }

    public void run() {
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
        while (true) {
            System.out.print("All (a), Detailed (d), range (r), or unfinished (u) query? ");
            String type = scanner.nextLine();
            if ("".equals(type)) {
                break;
            }
            if (!type.equalsIgnoreCase("d") && !type.equalsIgnoreCase("r")
                    && !type.equalsIgnoreCase("u") && !type.equalsIgnoreCase("a")) {
                continue;
            }
            if (type.equalsIgnoreCase("a")) {
                handleAllQuery();
            } else if (type.equalsIgnoreCase("d")) {
                handleDetailedQuery(scanner);
            } else if (type.equalsIgnoreCase("r")) {
                handleRangeQuery(scanner);
            } else {
                handleUnfinishedQuery();
            }
        }
    }

    public void parseQueryParameters(QueryType queryType, String queryParameters) {
        if (queryType.equals(QueryType.UNFINISHED)) {
            handleUnfinishedQuery();
        } else if (queryType.equals(QueryType.DETAILED)) {
            List<String> jobIds = Collections.singletonList(queryParameters);
            handleDetailedQuery(jobIds);
        } else if (queryType.equals(QueryType.RANGE)) {
            Instant startRange;
            Instant endRange;
            try {
                Date startRangeDate = dateInputFormat.parse(queryParameters.split(",")[0]);
                startRange = startRangeDate.toInstant();
                Date endRangeDate = dateInputFormat.parse(queryParameters.split(",")[1]);
                endRange = endRangeDate.toInstant();
            } catch (ParseException e) {
                System.out.println("Error while parsing input string, using system defaults (past 4 hours)");
                startRange = DEFAULT_RANGE_START;
                endRange = DEFAULT_RANGE_END;
            }
            handleRangeQuery(startRange, endRange);
        } else if (queryType.equals(QueryType.ALL)) {
            handleAllQuery();
        }
    }

    public void handleUnfinishedQuery() {
        List<CompactionJobStatus> statusList = compactionJobStatusCollector.runUnfinishedQuery();
        compactionJobStatusReporter.report(statusList, QueryType.UNFINISHED);
    }

    public void handleRangeQuery(Scanner scanner) {
        Instant startTime = promptForStartRange(scanner);
        Instant endTime = promptForEndRange(scanner);
        handleRangeQuery(startTime, endTime);
    }

    public void handleRangeQuery(Instant startRange, Instant endRange) {
        if (startRange.isAfter(endRange)) {
            System.out.println("Start range provided is before end range. Using system defaults (past 4 hours)");
            startRange = DEFAULT_RANGE_START;
            endRange = DEFAULT_RANGE_END;
        }
        List<CompactionJobStatus> statusList = compactionJobStatusCollector.runRangeQuery(startRange, endRange);
        compactionJobStatusReporter.report(statusList, QueryType.RANGE);
    }

    private Instant promptForStartRange(Scanner scanner) {
        return promptForRange(scanner, "start", DEFAULT_RANGE_START);
    }

    private Instant promptForEndRange(Scanner scanner) {
        return promptForRange(scanner, "end", DEFAULT_RANGE_END);
    }

    private Instant promptForRange(Scanner scanner, String rangeName, Instant defaultRange) {
        while (true) {
            System.out.printf("Enter %s range in format %s (default is %s):",
                    rangeName,
                    dateInputFormat.toPattern(),
                    dateInputFormat.format(Date.from(defaultRange)));
            String time = scanner.nextLine();
            if ("".equals(time)) {
                System.out.printf("Using default %s range %s%n", rangeName, dateInputFormat.format(Date.from(defaultRange)));
                break;
            }
            try {
                Date date = dateInputFormat.parse(time);
                return date.toInstant();
            } catch (ParseException e) {
                System.out.println("Error while parsing input string");
            }
        }
        return defaultRange;
    }

    public void handleDetailedQuery(Scanner scanner) {
        List<String> jobIds;

        System.out.print("Enter jobId to get detailed information about:");
        String input = scanner.nextLine();
        if ("".equals(input)) {
            return;
        }
        jobIds = Collections.singletonList(input);

        handleDetailedQuery(jobIds);
    }

    public void handleDetailedQuery(List<String> jobIds) {
        List<CompactionJobStatus> statusList = compactionJobStatusCollector.runDetailedQuery(jobIds);
        compactionJobStatusReporter.report(statusList, QueryType.DETAILED);
    }

    public void handleAllQuery() {
        List<CompactionJobStatus> statusList = compactionJobStatusCollector.runAllQuery();
        compactionJobStatusReporter.report(statusList, QueryType.ALL);
    }

    /**
     * Command line interface to retrieve compaction job statistics.
     * Can define query type and parameters or interactive if no query type provided
     * Range example
     * java CompactionJobStatusReport "test-instance" "standard" -r 20220927120000,20220927184500
     * Interactive example
     * java CompactionJobStatusReport "test-instance" "standard"
     *
     * @param args -
     * @throws IOException -
     */
    public static void main(String[] args) throws IOException {
        CompactionJobStatusReportArguments arguments;
        try {
            arguments = new CompactionJobStatusReportArguments(args);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            CompactionJobStatusReportArguments.printUsage(System.out);
            System.exit(1);
            return;
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, arguments.getInstanceId());

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        CompactionJobStatusReport statusReport = new CompactionJobStatusReport(dynamoDBClient, instanceProperties, arguments);
        if (null != arguments.getQueryType()) {
            statusReport.parseQueryParameters(arguments.getQueryType(), arguments.getQueryParameters());
        } else {
            statusReport.run();
        }
    }
}
