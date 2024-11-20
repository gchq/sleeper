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

package sleeper.systemtest.dsl;

import sleeper.systemtest.dsl.instance.DeployedSleeperInstances;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.util.TestContext;

/**
 * Tracks the context of a single running system test that will use the DSL. This is anything that needs to be
 * remembered from one step to the next, but not in between tests.
 * <p>
 * The {@link SleeperSystemTest} DSL uses this for the context of a test. Deployed resources and Sleeper instances are
 * managed separately in {@link SystemTestDeploymentContext}, outside of any test.
 */
public class SystemTestContext {
    private final SystemTestParameters parameters;
    private final DeployedSystemTestResources systemTestResources;
    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext sourceFiles;
    private final ReportingContext reporting;

    public SystemTestContext(
            SystemTestParameters parameters, SystemTestDrivers drivers,
            DeployedSystemTestResources systemTestResources, DeployedSleeperInstances deployedInstances,
            TestContext testContext) {
        this.parameters = parameters;
        this.systemTestResources = systemTestResources;
        instance = new SystemTestInstanceContext(parameters, deployedInstances, drivers.instance(parameters), testContext);
        sourceFiles = new IngestSourceFilesContext(systemTestResources, instance);
        reporting = new ReportingContext(parameters);
    }

    public SystemTestParameters parameters() {
        return parameters;
    }

    public DeployedSystemTestResources systemTest() {
        return systemTestResources;
    }

    public SystemTestInstanceContext instance() {
        return instance;
    }

    public IngestSourceFilesContext sourceFiles() {
        return sourceFiles;
    }

    public ReportingContext reporting() {
        return reporting;
    }
}
