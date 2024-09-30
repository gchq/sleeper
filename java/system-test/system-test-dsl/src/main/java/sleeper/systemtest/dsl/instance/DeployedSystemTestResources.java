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

package sleeper.systemtest.dsl.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.function.Consumer;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;

public class DeployedSystemTestResources {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployedSystemTestResources.class);

    private final SystemTestParameters parameters;
    private final SystemTestDeploymentDriver driver;
    private SystemTestStandaloneProperties properties;
    private InstanceDidNotDeployException failure;

    public DeployedSystemTestResources(SystemTestParameters parameters, SystemTestDeploymentDriver driver) {
        this.parameters = parameters;
        this.driver = driver;
    }

    public void updateProperties(Consumer<SystemTestStandaloneProperties> config) {
        config.accept(properties);
        driver.saveProperties(properties);
    }

    public SystemTestStandaloneProperties getProperties() {
        return properties;
    }

    public void deployIfMissing() throws InterruptedException {
        if (properties != null) {
            return;
        }
        if (failure != null) {
            throw failure;
        }
        try {
            deployIfMissingNoFailureTracking();
        } catch (RuntimeException | InterruptedException e) {
            failure = new InstanceDidNotDeployException(parameters.getSystemTestShortId(), e);
            throw e;
        }
    }

    public void resetProperties() {
        updateProperties(properties -> properties.getPropertiesIndex().getUserDefined().stream()
                .filter(property -> property.isEditable() && !property.isRunCdkDeployWhenChanged())
                .forEach(properties::unset));
    }

    public boolean isSystemTestClusterEnabled() {
        return parameters.isSystemTestClusterEnabled() && properties.getBoolean(SYSTEM_TEST_CLUSTER_ENABLED);
    }

    private void deployIfMissingNoFailureTracking() throws InterruptedException {
        boolean newDeployment = driver.deployIfNotPresent(parameters.buildSystemTestStandaloneProperties());
        properties = driver.loadProperties();
        if (!newDeployment && isRedeployNeeded()) {
            driver.redeploy(properties);
            properties = driver.loadProperties();
        }
    }

    private boolean isRedeployNeeded() {
        boolean redeployNeeded = false;
        if (parameters.isSystemTestClusterEnabled() && !properties.getBoolean(SYSTEM_TEST_CLUSTER_ENABLED)) {
            properties.set(SYSTEM_TEST_CLUSTER_ENABLED, "true");
            LOGGER.info("System test cluster not present, deploying");
            redeployNeeded = true;
        }
        if (parameters.isForceRedeploySystemTest()) {
            LOGGER.info("Forcing redeploy");
            redeployNeeded = true;
        }
        return redeployNeeded;
    }

    public String getSystemTestBucketName() {
        return properties.get(SYSTEM_TEST_BUCKET_NAME);
    }
}
