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

package sleeper.systemtest.dsl.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.systemtest.configurationv2.SystemTestStandaloneProperties;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;

public class DeployedSystemTestResources {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployedSystemTestResources.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final SystemTestParameters parameters;
    private final SystemTestDeploymentDriver driver;
    private SystemTestStandaloneProperties properties;
    private InstanceDidNotDeployException failure;

    public DeployedSystemTestResources(SystemTestParameters parameters, SystemTestDeploymentDriver driver) {
        this.parameters = parameters;
        this.driver = driver;
    }

    public SystemTestStandaloneProperties getProperties() {
        return properties;
    }

    public String getSystemTestBucketName() {
        return properties.get(SYSTEM_TEST_BUCKET_NAME);
    }

    public boolean isSystemTestClusterEnabled() {
        return parameters.isSystemTestClusterEnabled() && properties.getBoolean(SYSTEM_TEST_CLUSTER_ENABLED);
    }

    public void deployIfMissing() throws InterruptedException, ExecutionException {
        if (isDeployed()) {
            return;
        }
        Future<?> future = executor.submit(() -> {
            if (isDeployed()) {
                return;
            }
            try {
                deployIfMissingNoFailureTracking();
            } catch (RuntimeException | InterruptedException e) {
                failure = new InstanceDidNotDeployException(parameters.getSystemTestShortId(), e);
                throw failure;
            }
        });
        future.get();
    }

    private boolean isDeployed() {
        if (failure != null) {
            throw failure;
        }
        return properties != null;
    }

    private void deployIfMissingNoFailureTracking() throws InterruptedException {
        boolean newDeployment = driver.deployIfNotPresent(parameters.buildSystemTestStandaloneProperties());
        properties = driver.loadProperties();
        if (!newDeployment && isRedeployNeeded()) {
            driver.redeploy(properties);
            properties = driver.loadProperties();
        }
        resetProperties();
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

    private void resetProperties() {
        properties.getPropertiesIndex().getUserDefined().stream()
                .filter(property -> property.isEditable() && !property.isRunCdkDeployWhenChanged())
                .forEach(properties::unset);
        driver.saveProperties(properties);
    }
}
