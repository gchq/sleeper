/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.systemtest.dsl.extension;

import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.util.TestContext;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class AfterTestReportsBase<B extends SystemTestReports.Builder> {

    private final Supplier<B> reportsFactory;
    private boolean reportIfPassed;
    private Consumer<B> config;

    public AfterTestReportsBase(Supplier<B> reportsFactory) {
        this.reportsFactory = reportsFactory;
    }

    public void reportAlways(Consumer<B> config) {
        reportIfPassed(true, config);
    }

    public void reportIfTestFailed(Consumer<B> config) {
        reportIfPassed(false, config);
    }

    private void reportIfPassed(boolean reportIfPassed, Consumer<B> config) {
        this.reportIfPassed = reportIfPassed;
        this.config = config;
    }

    public void afterTestPassed(TestContext testContext) {
        if (reportIfPassed) {
            reports().print(testContext);
        }
    }

    public void afterTestFailed(TestContext testContext) {
        if (config != null) {
            reports().print(testContext);
        }
    }

    private SystemTestReports reports() {
        B builder = reportsFactory.get();
        config.accept(builder);
        return builder.build();
    }
}
