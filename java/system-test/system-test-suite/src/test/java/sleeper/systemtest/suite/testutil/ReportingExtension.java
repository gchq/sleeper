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

package sleeper.systemtest.suite.testutil;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import sleeper.systemtest.drivers.instance.ReportingContext;
import sleeper.systemtest.drivers.instance.SystemTestReport;
import sleeper.systemtest.drivers.util.TestContext;

import java.util.List;

import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

public class ReportingExtension implements BeforeEachCallback, AfterEachCallback {

    private final ReportingContext context;
    private final List<SystemTestReport> reports;
    private final boolean reportIfPassed;

    public ReportingExtension(ReportingContext context, List<SystemTestReport> reports, boolean reportIfPassed) {
        this.context = context;
        this.reports = reports;
        this.reportIfPassed = reportIfPassed;
    }

    public static ReportingExtension reportAlways(ReportingContext context, SystemTestReport... reports) {
        return new ReportingExtension(context, List.of(reports), true);
    }

    public static ReportingExtension reportIfFailed(ReportingContext context, SystemTestReport... reports) {
        return new ReportingExtension(context, List.of(reports), false);
    }

    @Override
    public void beforeEach(ExtensionContext testContext) {
        context.startRecording();
    }

    @Override
    public void afterEach(ExtensionContext testContext) {
        if (testContext.getExecutionException().isPresent()) {
            afterTestFailed(testContext(testContext));
        } else {
            afterTestPassed(testContext(testContext));
        }
    }

    public void afterTestPassed(TestContext testContext) {
        if (reportIfPassed) {
            printReports(testContext);
        }
    }

    public void afterTestFailed(TestContext testContext) {
        printReports(testContext);
    }

    private void printReports(TestContext testContext) {
        context.print(testContext, (out, startTime) ->
                reports.forEach(report -> report.print(out, startTime)));
    }
}
