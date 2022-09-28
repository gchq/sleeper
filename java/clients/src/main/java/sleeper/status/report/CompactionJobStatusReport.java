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
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.status.report.compactionjob.CompactionJobStatusCollector;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;
import sleeper.status.report.compactionjob.JsonCompactionJobStatusReporter;
import sleeper.status.report.compactionjob.StandardCompactionJobStatusReporter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class CompactionJobStatusReport {
    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobStatusCollector compactionJobStatusCollector;
    private static final String DEFAULT_STATUS_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> FILE_STATUS_REPORTERS = new HashMap<>();
    private final SimpleDateFormat dateInputFormat = new SimpleDateFormat("yyyyMMddhhmmss");
    private static final Instant DEFAULT_RANGE_START = Instant.now().minus(4L, ChronoUnit.HOURS);
    private static final Instant DEFAULT_RANGE_END = Instant.now();

    static {
        FILE_STATUS_REPORTERS.put(DEFAULT_STATUS_REPORTER, new StandardCompactionJobStatusReporter());
        FILE_STATUS_REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    private static final Map<String, QueryType> QUERY_TYPES = new HashMap<>();

    static {
        QUERY_TYPES.put("-d", QueryType.DETAILED);
        QUERY_TYPES.put("-r", QueryType.RANGE);
        QUERY_TYPES.put("-u", QueryType.UNFINISHED);
    }

    public CompactionJobStatusReport(AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties, String outputType) {
        String instanceId = instanceProperties.get(ID);
        String tableName = DynamoDBCompactionJobStatusStore.jobStatusTableName(instanceId);
        CompactionJobStatusStore compactionJobStatusStore = DynamoDBCompactionJobStatusStore.from(dynamoDB, instanceProperties);
        compactionJobStatusCollector = new CompactionJobStatusCollector(compactionJobStatusStore, tableName);
        if (!FILE_STATUS_REPORTERS.containsKey(outputType)) {
            throw new IllegalArgumentException("Output type not supported " + outputType);
        }
        compactionJobStatusReporter = FILE_STATUS_REPORTERS.get(outputType);
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
        if (!(args.length >= 1 && args.length <= 4)) {
            throw new IllegalArgumentException("Usage: <instance id> <report_type_standard_or_json> <optional_query_type> <optional_query_parameters> \n" +
                    "Query types are:\n" +
                    "-a (Return all jobs)\n" +
                    "-d (Detailed, provide a jobId)\n" +
                    "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                    "-u (Unfinished jobs)");
        }
        String instanceId = args[0];
        String reporterType = DEFAULT_STATUS_REPORTER;
        if (args.length >= 2) {
            reporterType = args[1].toUpperCase(Locale.ROOT);
        }
        QueryType queryType = null;
        String queryParameters = null;
        if (args.length >= 3) {
            if (!QUERY_TYPES.containsKey(args[2])) {
                throw new IllegalArgumentException("Invalid query type " + args[2] + ". Valid query types are -d (Detailed), -r (Range), -u (Unfinished)");
            }
            queryType = QUERY_TYPES.get(args[2]);
            if (args.length >= 4 && !(queryType.equals(QueryType.UNFINISHED) || queryType.equals(QueryType.ALL))) {
                throw new IllegalArgumentException("No parameters provided for query type " + queryType);
            }
            queryParameters = "";
            if (args.length >= 4) {
                queryParameters = args[3];
            }
        }

//        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
//        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, instanceId);
//
//        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        CompactionJobStatusReport statusReport = new CompactionJobStatusReport(null, new InstanceProperties(), reporterType);
        if (null != queryType) {
            statusReport.parseQueryParameters(queryType, queryParameters);
        } else {
            statusReport.run();
        }
    }
}
