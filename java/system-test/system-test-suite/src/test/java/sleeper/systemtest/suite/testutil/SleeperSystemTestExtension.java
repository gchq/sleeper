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

package sleeper.systemtest.suite.testutil;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import sleeper.systemtest.drivers.instance.AwsSystemTestParameters;
import sleeper.systemtest.drivers.util.AwsSystemTestDrivers;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.util.SystemTestDrivers;

import java.util.Set;

import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

public class SleeperSystemTestExtension implements ParameterResolver, BeforeEachCallback, AfterEachCallback {

    private static final SystemTestParameters PARAMETERS = AwsSystemTestParameters.loadFromSystemProperties();
    private static final SystemTestDrivers DRIVERS = new AwsSystemTestDrivers(PARAMETERS);
    private static final SleeperSystemTest DSL = new SleeperSystemTest(PARAMETERS, DRIVERS);
    private AfterTestReports reporting;
    private AfterTestPurgeQueues queuePurging;

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return Set.of(SleeperSystemTest.class, AfterTestReports.class, AfterTestPurgeQueues.class)
                .contains(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        if (type == SleeperSystemTest.class) {
            return DSL;
        } else if (type == AfterTestReports.class) {
            return reporting;
        } else if (type == AfterTestPurgeQueues.class) {
            return queuePurging;
        } else {
            throw new IllegalStateException("Unsupported parameter type: " + type);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        DSL.reset();
        reporting = new AfterTestReports(DRIVERS);
        queuePurging = new AfterTestPurgeQueues(DRIVERS.purgeQueueDriver());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (context.getExecutionException().isPresent()) {
            reporting.afterTestFailed(testContext(context));
            queuePurging.testFailed();
        } else {
            reporting.afterTestPassed(testContext(context));
            queuePurging.testPassed();
        }
    }
}
