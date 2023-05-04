/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.output;

import com.google.gson.Gson;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.clients.util.GsonConfig;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableWriter;
import sleeper.clients.util.table.TableWriterFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class NightlyTestSummaryTable {

    private static final Gson GSON = GsonConfig.standardBuilder().create();

    private final List<Execution> executions = new ArrayList<>();

    private NightlyTestSummaryTable() {
    }

    public static NightlyTestSummaryTable empty() {
        return new NightlyTestSummaryTable();
    }

    public static NightlyTestSummaryTable fromJson(String json) {
        return GSON.fromJson(json, NightlyTestSummaryTable.class);
    }

    public NightlyTestSummaryTable add(
            NightlyTestTimestamp timestamp, NightlyTestOutput output) {
        executions.add(execution(timestamp, output));
        return this;
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public String toTableString() {
        OutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream, false, StandardCharsets.UTF_8);
        TableWriterFactory.Builder tableDefinitionBuilder = TableWriterFactory.builder();
        TableField startTime = tableDefinitionBuilder.addField("START_TIME");
        Map<String, TableField> fieldByTestName = executions.stream()
                .flatMap(execution -> execution.tests.stream())
                .map(test -> test.name).distinct()
                .collect(Collectors.toMap(name -> name, tableDefinitionBuilder::addField));
        TableWriter.Builder tableBuilder = tableDefinitionBuilder.build().tableBuilder();
        executions.forEach(execution -> tableBuilder.row(rowBuilder -> {
            rowBuilder.value(startTime, execution.startTime);
            execution.tests.forEach(test -> rowBuilder.value(fieldByTestName.get(test.name), getTestStatus(test.exitCode)));
        }));
        tableBuilder.build().write(printStream);
        return outputStream.toString();
    }

    private String getTestStatus(Integer exitCode) {
        if (exitCode == 0) {
            return "PASSED";
        } else {
            return "FAILED";
        }
    }

    private static Execution execution(NightlyTestTimestamp timestamp, NightlyTestOutput output) {
        return new Execution(timestamp.toInstant(), tests(output.getStatusCodeByTest()));
    }

    private static List<Test> tests(Map<String, Integer> statusCodeByTest) {
        return statusCodeByTest.entrySet().stream()
                .map(entry -> new Test(entry.getKey(), entry.getValue()))
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

        public Test(String name, Integer exitCode) {
            this.name = name;
            this.exitCode = exitCode;
        }
    }

}
