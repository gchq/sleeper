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
import sleeper.status.report.compaction.job.CompactionJobQuery;
import sleeper.status.report.compaction.job.CompactionJobStatusReportArguments;
import sleeper.status.report.compaction.job.CompactionJobStatusReporter;
import sleeper.status.report.compaction.job.query.AllCompactionJobQuery;
import sleeper.status.report.compaction.job.query.DetailedCompactionJobQuery;
import sleeper.status.report.compaction.job.query.RangeCompactionJobQuery;
import sleeper.status.report.compaction.job.query.UnfinishedCompactionJobQuery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.Scanner;
import java.util.TimeZone;

public class CompactionJobStatusReport {
    private final CompactionJobStatusReportArguments arguments;
    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobStatusStore compactionJobStatusStore;
    private static final String DATE_FORMAT = "yyyyMMddhhmmss";
    private static final Instant DEFAULT_RANGE_START = Instant.now().minus(4L, ChronoUnit.HOURS);
    private static final Instant DEFAULT_RANGE_END = Instant.now();

    private static SimpleDateFormat createDateInputFormat() {
        SimpleDateFormat dateInputFormat = new SimpleDateFormat(DATE_FORMAT);
        dateInputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateInputFormat;
    }

    public static Instant parseDate(String input) throws ParseException {
        return createDateInputFormat().parse(input).toInstant();
    }

    public static String formatDate(Instant input) {
        return createDateInputFormat().format(Date.from(input));
    }

    public CompactionJobStatusReport(
            CompactionJobStatusStore compactionJobStatusStore,
            CompactionJobStatusReportArguments arguments) {
        this.arguments = arguments;
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.compactionJobStatusReporter = arguments.getReporter();
    }

    public void run() {
        CompactionJobQuery query;
        if (arguments.isPromptForQuery()) {
            query = promptForQuery();
        } else {
            query = arguments.buildQuery(Clock.systemUTC());
        }
        if (query == null) {
            return;
        }
        compactionJobStatusReporter.report(query.run(compactionJobStatusStore), arguments.getQueryType());
    }

    private CompactionJobQuery promptForQuery() {
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.displayName());
        while (true) {
            System.out.print("All (a), Detailed (d), range (r), or unfinished (u) query? ");
            String type = scanner.nextLine();
            if ("".equals(type)) {
                return null;
            }
            if (type.equalsIgnoreCase("a")) {
                return new AllCompactionJobQuery(arguments.getTableName());
            } else if (type.equalsIgnoreCase("d")) {
                System.out.print("Enter jobId to get detailed information about:");
                String input = scanner.nextLine();
                if ("".equals(input)) {
                    return null;
                }
                return new DetailedCompactionJobQuery(Collections.singletonList(input));
            } else if (type.equalsIgnoreCase("r")) {
                Instant startTime = promptForStartRange(scanner);
                Instant endTime = promptForEndRange(scanner);
                return new RangeCompactionJobQuery(arguments.getTableName(), startTime, endTime);
            } else if (type.equalsIgnoreCase("u")) {
                return new UnfinishedCompactionJobQuery(arguments.getTableName());
            }
        }
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
                    DATE_FORMAT,
                    formatDate(defaultRange));
            String time = scanner.nextLine();
            if ("".equals(time)) {
                System.out.printf("Using default %s range %s%n", rangeName, formatDate(defaultRange));
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
