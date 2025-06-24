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

package sleeper.systemtest.drivers.nightly;

import com.google.gson.Gson;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import sleeper.clients.util.ClientsGsonConfig;
import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.clients.util.tablewriter.TableWriterFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class NightlyTestSummaryTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NightlyTestSummaryTable.class);

    private static final Gson GSON = ClientsGsonConfig.standardBuilder().create();

    private final LinkedList<Execution> executions = new LinkedList<>();

    private NightlyTestSummaryTable() {
    }

    public static NightlyTestSummaryTable empty() {
        return new NightlyTestSummaryTable();
    }

    public static NightlyTestSummaryTable fromJson(String json) {
        return GSON.fromJson(json, NightlyTestSummaryTable.class);
    }

    public static NightlyTestSummaryTable fromS3(S3Client s3Client, String bucketName) {
        LOGGER.info("Loading existing test summary from S3");
        if (doesObjectExist(s3Client, bucketName, "summary.json")) {
            String json = s3Client.getObject(
                    request -> request.bucket(bucketName).key("summary.json"),
                    ResponseTransformer.toBytes())
                    .asUtf8String();
            NightlyTestSummaryTable summary = fromJson(json);
            LOGGER.info("Found test summary with {} executions", summary.executions.size());
            return summary;
        } else {
            LOGGER.info("Found no test summary");
            return empty();
        }
    }

    private static boolean doesObjectExist(S3Client s3Client, String bucketName, String objectKey) {
        try {
            s3Client.headObject(request -> request.bucket(bucketName).key(objectKey));
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    public void saveToS3(S3Client s3Client, String bucketName) {
        LOGGER.info("Saving test summary with {} executions to S3 bucket: {}", executions.size(), bucketName);
        s3Client.putObject(
                request -> request.bucket(bucketName).key("summary.json"),
                RequestBody.fromString(toJson()));
        s3Client.putObject(
                request -> request.bucket(bucketName).key("summary.txt"),
                RequestBody.fromString(toTableString()));
        LOGGER.info("Saved test summary to S3");
    }

    public NightlyTestSummaryTable add(
            NightlyTestTimestamp timestamp, NightlyTestOutput output) {
        executions.addFirst(execution(timestamp, output));
        return this;
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public String toTableString() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream, false, StandardCharsets.UTF_8);
        printTableString(printStream);
        return outputStream.toString(StandardCharsets.UTF_8);
    }

    private void printTableString(PrintStream printStream) {
        TableWriterFactory.Builder tableDefinitionBuilder = TableWriterFactory.builder();

        TableField startTimeField = tableDefinitionBuilder.addField("START_TIME");
        Map<String, TableField> fieldByTestName = addTestFieldsToTable(tableDefinitionBuilder);

        TableWriter.Builder tableBuilder = tableDefinitionBuilder.build().tableBuilder();

        addDataToTable(startTimeField, fieldByTestName, tableBuilder);

        tableBuilder.build().write(printStream);
    }

    private Map<String, TableField> addTestFieldsToTable(TableWriterFactory.Builder tableDefinitionBuilder) {
        return executions.stream()
                .flatMap(execution -> execution.tests.stream())
                .map(test -> test.name).distinct()
                .collect(Collectors.toMap(name -> name, tableDefinitionBuilder::addField));
    }

    private void addDataToTable(
            TableField startTimeField, Map<String, TableField> fieldByTestName, TableWriter.Builder tableBuilder) {
        executions.forEach(execution -> tableBuilder.row(rowBuilder -> {
            rowBuilder.value(startTimeField, execution.startTime);
            execution.tests.forEach(test -> rowBuilder.value(fieldByTestName.get(test.name), getTestStatus(test.exitCode)));
        }));
    }

    private String getTestStatus(Integer exitCode) {
        if (exitCode == 0) {
            return "PASSED";
        } else {
            return "FAILED";
        }
    }

    private static Execution execution(NightlyTestTimestamp timestamp, NightlyTestOutput output) {
        return new Execution(timestamp.toInstant(), tests(output.getTests()));
    }

    private static List<Test> tests(List<TestResult> testResults) {
        return testResults.stream()
                .map(result -> new Test(result.getTestName(), result.getExitCode(), result.getInstanceId()))
                .sorted(Comparator.comparing(o -> o.name))
                .collect(Collectors.toList());
    }

    @SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
    public static class Execution {

        private final Instant startTime;
        private final List<Test> tests;

        public Execution(Instant startTime, List<Test> tests) {
            this.startTime = startTime;
            this.tests = tests;
        }
    }

    @SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
    public static class Test {

        private final String name;
        private final Integer exitCode;
        private final String instanceId;

        public Test(String name, Integer exitCode, String instanceId) {
            this.name = name;
            this.exitCode = exitCode;
            this.instanceId = instanceId;
        }
    }
}
