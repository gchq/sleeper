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
import java.util.TimeZone;

public class CompactionJobStatusReport {
    private final CompactionJobStatusReportArguments arguments;
    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobStatusCollector compactionJobStatusCollector;
    private static final SimpleDateFormat DATE_INPUT_FORMAT = new SimpleDateFormat("yyyyMMddhhmmss");

    static {
        DATE_INPUT_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static final Instant DEFAULT_RANGE_START = Instant.now().minus(4L, ChronoUnit.HOURS);
    private static final Instant DEFAULT_RANGE_END = Instant.now();

    public static Instant parseDate(String input) throws ParseException {
        return DATE_INPUT_FORMAT.parse(input).toInstant();
    }

    public CompactionJobStatusReport(
            CompactionJobStatusStore compactionJobStatusStore,
            CompactionJobStatusReportArguments arguments) {
        this.arguments = arguments;
        this.compactionJobStatusCollector = new CompactionJobStatusCollector(compactionJobStatusStore, arguments.getTableName());
        this.compactionJobStatusReporter = arguments.getReporter();
    }

    public void run() {
        switch (arguments.getQueryType()) {
            case PROMPT:
                runWithPrompts();
                break;
            case UNFINISHED:
                handleUnfinishedQuery();
                break;
            case DETAILED:
                List<String> jobIds = Collections.singletonList(arguments.getQueryParameters());
                handleDetailedQuery(jobIds);
                break;
            case RANGE:
                Instant startRange;
                Instant endRange;
                try {
                    startRange = parseDate(arguments.getQueryParameters().split(",")[0]);
                    endRange = parseDate(arguments.getQueryParameters().split(",")[1]);
                } catch (ParseException e) {
                    System.out.println("Error while parsing input string, using system defaults (past 4 hours)");
                    startRange = DEFAULT_RANGE_START;
                    endRange = DEFAULT_RANGE_END;
                }
                handleRangeQuery(startRange, endRange);
                break;
            case ALL:
                handleAllQuery();
                break;
            default:
                throw new IllegalArgumentException("Unexpected query type: " + arguments.getQueryType());
        }
    }

    private void runWithPrompts() {
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
        while (true) {
            System.out.print("All (a), Detailed (d), range (r), or unfinished (u) query? ");
            String type = scanner.nextLine();
            if ("".equals(type)) {
                break;
            }
            if (type.equalsIgnoreCase("a")) {
                handleAllQuery();
            } else if (type.equalsIgnoreCase("d")) {
                handleDetailedQuery(scanner);
            } else if (type.equalsIgnoreCase("r")) {
                handleRangeQuery(scanner);
            } else if (type.equalsIgnoreCase("u")) {
                handleUnfinishedQuery();
            }
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
                    DATE_INPUT_FORMAT.toPattern(),
                    DATE_INPUT_FORMAT.format(Date.from(defaultRange)));
            String time = scanner.nextLine();
            if ("".equals(time)) {
                System.out.printf("Using default %s range %s%n", rangeName, DATE_INPUT_FORMAT.format(Date.from(defaultRange)));
                break;
            }
            try {
                return parseDate(time);
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

    public static void main(String[] args) throws IOException {
        CompactionJobStatusReportArguments arguments;
        try {
            arguments = CompactionJobStatusReportArguments.from(args);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            CompactionJobStatusReportArguments.printUsage(System.out);
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
