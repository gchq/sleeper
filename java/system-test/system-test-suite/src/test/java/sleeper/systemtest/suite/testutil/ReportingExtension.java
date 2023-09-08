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

import java.util.List;

import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

public class ReportingExtension implements BeforeEachCallback, AfterEachCallback {

    private final ReportingContext context;
    private final List<SystemTestReport> reports;

    public ReportingExtension(ReportingContext context, List<SystemTestReport> reports) {
        this.context = context;
        this.reports = reports;
    }

    @Override
    public void beforeEach(ExtensionContext testContext) {
        context.startRecording();
    }

    @Override
    public void afterEach(ExtensionContext testContext) {
        context.print(testContext(testContext), (out, startTime) ->
                reports.forEach(report -> report.print(out, startTime)));
    }
}
