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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.DeployedSleeperInstances;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.util.TestContext;

import java.util.Set;

public class SleeperSystemTestExtension implements ParameterResolver, BeforeAllCallback, BeforeEachCallback, AfterEachCallback {

    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperSystemTestExtension.class);
    private static final Set<Class<?>> SUPPORTED_PARAMETER_TYPES = Set.of(
            SleeperSystemTest.class, AfterTestReports.class, AfterTestPurgeQueues.class,
            SystemTestParameters.class, SystemTestDrivers.class,
            DeployedSystemTestResources.class, DeployedSleeperInstances.class,
            SystemTestContext.class);

    private final SystemTestParameters parameters;
    private final SystemTestDrivers drivers;
    private final DeployedSystemTestResources deployedResources;
    private final DeployedSleeperInstances deployedInstances;
    private SystemTestContext testContext = null;
    private SleeperSystemTest dsl = null;
    private AfterTestReports reporting = null;
    private AfterTestPurgeQueues queuePurging = null;

    protected SleeperSystemTestExtension(SystemTestDeploymentContext context) {
        parameters = context.parameters();
        drivers = context.drivers();
        deployedResources = context.deployedResources();
        deployedInstances = context.deployedInstances();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        return SUPPORTED_PARAMETER_TYPES.contains(type)
                || drivers.getClass() == type;
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
        } else if (type == SystemTestParameters.class) {
            return parameters;
        } else if (type.isAssignableFrom(drivers.getClass())) {
            return drivers;
        } else if (type == DeployedSystemTestResources.class) {
            return deployedResources;
        } else if (type == DeployedSleeperInstances.class) {
            return deployedInstances;
        } else if (type == SystemTestContext.class) {
            return testContext;
        } else {
            throw new IllegalStateException("Unsupported parameter type: " + type);
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        deployedResources.deployIfMissing();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        LOGGER.info("Beginning system test: {}",
                TestContextFactory.testContext(context).getTestClassAndMethod());
        deployedResources.resetProperties();
        drivers.generatedSourceFiles(parameters, deployedResources).emptyBucket();
        testContext = new SystemTestContext(parameters, drivers, deployedResources, deployedInstances);
        dsl = new SleeperSystemTest(parameters, drivers, testContext);
        reporting = new AfterTestReports(testContext);
        queuePurging = new AfterTestPurgeQueues(testContext);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        TestContext testContext = TestContextFactory.testContext(context);
        context.getExecutionException().ifPresentOrElse(failure -> {
            reporting.afterTestFailed(testContext);
            queuePurging.testFailed();
            LOGGER.error("Failed system test: {}", testContext.getTestClassAndMethod(), failure);
        }, () -> {
            reporting.afterTestPassed(testContext);
            queuePurging.testPassed();
            LOGGER.info("Passed system test: {}", testContext.getTestClassAndMethod());
        });
        dsl = null;
        reporting = null;
        queuePurging = null;
    }
}
