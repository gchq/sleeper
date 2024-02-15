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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.DeployedSleeperInstances;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.util.Set;

import static sleeper.systemtest.dsl.extension.TestContextFactory.testContext;

public class SleeperSystemTestExtension implements ParameterResolver, BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

    private final SystemTestParameters parameters;
    private final SystemTestDrivers drivers;
    private final DeployedSystemTestResources deployedResources;
    private final DeployedSleeperInstances deployedInstances;
    private SleeperSystemTest dsl = null;
    private AfterTestReports reporting = null;
    private AfterTestPurgeQueues queuePurging = null;

    protected SleeperSystemTestExtension(SystemTestParameters parameters, SystemTestDrivers drivers) {
        this.parameters = parameters;
        this.drivers = drivers;
        deployedResources = new DeployedSystemTestResources(parameters, drivers.systemTestDeployment(parameters));
        deployedInstances = new DeployedSleeperInstances(
                parameters, deployedResources, drivers.instance(parameters), drivers.tables(parameters));
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return Set.of(SleeperSystemTest.class, AfterTestReports.class, AfterTestPurgeQueues.class)
                .contains(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        if (type == SleeperSystemTest.class) {
            return dsl;
        } else if (type == AfterTestReports.class) {
            return reporting;
        } else if (type == AfterTestPurgeQueues.class) {
            return queuePurging;
        } else {
            throw new IllegalStateException("Unsupported parameter type: " + type);
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        deployedResources.deployIfMissing();
        deployedResources.resetProperties();
        drivers.generatedSourceFiles(parameters, deployedResources).emptyBucket();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        SystemTestContext testContext = new SystemTestContext(parameters, drivers, deployedResources, deployedInstances);
        dsl = new SleeperSystemTest(parameters, drivers, testContext);
        reporting = new AfterTestReports(drivers, testContext);
        queuePurging = new AfterTestPurgeQueues(drivers.purgeQueueDriver(testContext));
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
        dsl = null;
        reporting = null;
        queuePurging = null;
    }
}
