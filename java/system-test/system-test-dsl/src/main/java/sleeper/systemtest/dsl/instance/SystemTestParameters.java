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

import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.model.SleeperArtefactsLocation;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.systemtest.configuration.SystemTestProperty;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.util.function.Predicate.not;
import static sleeper.core.properties.instance.CommonProperty.ARTEFACTS_DEPLOYMENT_ID;
import static sleeper.core.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ACCOUNT;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID_MAX_LEN;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JARS_BUCKET;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_LOG_RETENTION_DAYS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REGION;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_VPC_ID;

public class SystemTestParameters {

    private final String shortTestId;
    private final String account;
    private final String region;
    private final String vpcId;
    private final String subnetIds;
    private final Path scriptsDirectory;
    private final Path outputDirectory;
    private final Path pythonDirectory;
    private final boolean systemTestClusterEnabled;
    private final boolean forceRedeploySystemTest;
    private final boolean forceRedeployInstances;
    private final String forceStateStoreClassname;
    private final boolean createMultiPlatformBuilder;
    private final SystemTestStandaloneProperties standalonePropertiesTemplate;
    private final InstanceProperties instancePropertiesOverrides;

    private SystemTestParameters(Builder builder) {
        shortTestId = Objects.requireNonNull(builder.shortTestId, "shortTestId must not be null");
        account = Objects.requireNonNull(builder.account, "account must not be null");
        region = Objects.requireNonNull(builder.region, "region must not be null");
        vpcId = Objects.requireNonNull(builder.vpcId, "vpcId must not be null");
        subnetIds = Objects.requireNonNull(builder.subnetIds, "subnetIds must not be null");
        scriptsDirectory = Objects.requireNonNull(builder.scriptsDirectory, "scriptsDirectory must not be null");
        outputDirectory = builder.outputDirectory;
        pythonDirectory = Objects.requireNonNull(builder.pythonDirectory, "pythonDirectory must not be null");
        systemTestClusterEnabled = builder.systemTestClusterEnabled;
        forceRedeploySystemTest = builder.forceRedeploySystemTest;
        forceRedeployInstances = builder.forceRedeployInstances;
        forceStateStoreClassname = builder.forceStateStoreClassname;
        createMultiPlatformBuilder = builder.createMultiPlatformBuilder;
        standalonePropertiesTemplate = Objects.requireNonNull(builder.standalonePropertiesTemplate, "standalonePropertiesTemplate must not be null");
        instancePropertiesOverrides = Objects.requireNonNull(builder.instancePropertiesOverrides, "instancePropertiesOverrides must not be null");
        // Combines with SystemTestInstanceConfiguration.shortName and a hyphen to create an instance ID within maximum length
        if (!SystemTestProperty.SYSTEM_TEST_ID.getValidationPredicate().test(shortTestId)) {
            throw new IllegalArgumentException("shortTestId is not valid, must be at most " + SYSTEM_TEST_ID_MAX_LEN + " characters: " + shortTestId);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSystemTestShortId() {
        return shortTestId;
    }

    public String getArtefactsDeploymentId() {
        return shortTestId;
    }

    public String buildInstanceId(String identifier) {
        return shortTestId + "-" + identifier;
    }

    public String getAccount() {
        return account;
    }

    public String getRegion() {
        return region;
    }

    public String getVpcId() {
        return vpcId;
    }

    public String getSubnetIds() {
        return subnetIds;
    }

    public Path getScriptsDirectory() {
        return scriptsDirectory;
    }

    public Path getJarsDirectory() {
        return scriptsDirectory.resolve("jars");
    }

    public Path getDockerDirectory() {
        return scriptsDirectory.resolve("docker");
    }

    public Path getGeneratedDirectory() {
        return scriptsDirectory.resolve("generated");
    }

    public Path getOutputDirectory() {
        return outputDirectory;
    }

    public Path getPythonDirectory() {
        return pythonDirectory;
    }

    public boolean isSystemTestClusterEnabled() {
        return systemTestClusterEnabled;
    }

    public boolean isForceRedeploySystemTest() {
        return forceRedeploySystemTest;
    }

    public boolean isForceRedeployInstances() {
        return forceRedeployInstances;
    }

    public boolean isCreateMultiPlatformBuilder() {
        return createMultiPlatformBuilder;
    }

    public TableProperties createTableProperties(InstanceProperties instanceProperties, Schema schema) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        tableProperties.set(TABLE_NAME, UUID.randomUUID().toString());
        setRequiredProperties(tableProperties);
        return tableProperties;
    }

    public void setRequiredProperties(SleeperInstanceConfiguration deployConfig) {
        InstanceProperties properties = deployConfig.getInstanceProperties();
        properties.set(ARTEFACTS_DEPLOYMENT_ID, getArtefactsDeploymentId());
        if (standalonePropertiesTemplate.isSet(SYSTEM_TEST_LOG_RETENTION_DAYS)) {
            properties.set(LOG_RETENTION_IN_DAYS, standalonePropertiesTemplate.get(SYSTEM_TEST_LOG_RETENTION_DAYS));
        }
        instancePropertiesOverrides.streamNonDefaultEntries().forEach(entry -> {
            properties.set(entry.getKey(), entry.getValue());
        });
        for (TableProperties tableProperties : deployConfig.getTableProperties()) {
            setRequiredProperties(tableProperties);
        }
    }

    private void setRequiredProperties(TableProperties tableProperties) {
        if (forceStateStoreClassname != null) {
            tableProperties.set(STATESTORE_CLASSNAME, forceStateStoreClassname);
        }
    }

    public SystemTestStandaloneProperties buildSystemTestStandaloneProperties() {
        SystemTestStandaloneProperties properties = SystemTestStandaloneProperties.copyOf(standalonePropertiesTemplate);
        properties.set(SYSTEM_TEST_ID, getSystemTestShortId());
        properties.set(SYSTEM_TEST_ACCOUNT, getAccount());
        properties.set(SYSTEM_TEST_REGION, getRegion());
        properties.set(SYSTEM_TEST_VPC_ID, getVpcId());
        properties.set(SYSTEM_TEST_JARS_BUCKET, SleeperArtefactsLocation.getDefaultJarsBucketName(getArtefactsDeploymentId()));
        properties.set(SYSTEM_TEST_REPO, SleeperArtefactsLocation.getDefaultEcrRepositoryPrefix(getArtefactsDeploymentId()) + "/system-test");
        properties.set(SYSTEM_TEST_CLUSTER_ENABLED, String.valueOf(isSystemTestClusterEnabled()));
        return properties;
    }

    public boolean isInstancePropertyOverridden(InstanceProperty property) {
        return instancePropertiesOverrides.isSet(property);
    }

    private static Path findScriptsDir() {
        return getParentOrFail(findJavaDir()).resolve("scripts");
    }

    private static Path findJavaDir() {
        return findJavaDir(Path.of(".").toAbsolutePath());
    }

    private static Path findJavaDir(Path currentPath) {
        if ("java".equals(String.valueOf(currentPath.getFileName()))) {
            return currentPath;
        } else {
            return findJavaDir(getParentOrFail(currentPath));
        }
    }

    private static Path findPythonDir() {
        return getParentOrFail(findJavaDir()).resolve("python");
    }

    private static Path getParentOrFail(Path path) {
        Path parent = path.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("No parent of path " + path);
        } else {
            return parent;
        }
    }

    public static final class Builder {
        private String shortTestId;
        private String account;
        private String region;
        private String vpcId;
        private String subnetIds;
        private Path scriptsDirectory;
        private Path outputDirectory;
        private Path pythonDirectory;
        private boolean systemTestClusterEnabled;
        private boolean forceRedeploySystemTest;
        private boolean forceRedeployInstances;
        private String forceStateStoreClassname;
        private boolean createMultiPlatformBuilder = true;
        private SystemTestStandaloneProperties standalonePropertiesTemplate;
        private InstanceProperties instancePropertiesOverrides;

        private Builder() {
        }

        public Builder shortTestId(String shortTestId) {
            this.shortTestId = shortTestId;
            return this;
        }

        public Builder account(String account) {
            this.account = account;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetIds(String subnetIds) {
            this.subnetIds = subnetIds;
            return this;
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder outputDirectory(Path outputDirectory) {
            this.outputDirectory = outputDirectory;
            return this;
        }

        public Builder pythonDirectory(Path pythonDirectory) {
            this.pythonDirectory = pythonDirectory;
            return this;
        }

        public Builder systemTestClusterEnabled(boolean systemTestClusterEnabled) {
            this.systemTestClusterEnabled = systemTestClusterEnabled;
            return this;
        }

        public Builder forceRedeploySystemTest(boolean forceRedeploySystemTest) {
            this.forceRedeploySystemTest = forceRedeploySystemTest;
            return this;
        }

        public Builder forceRedeployInstances(boolean forceRedeployInstances) {
            this.forceRedeployInstances = forceRedeployInstances;
            return this;
        }

        public Builder forceStateStoreClassname(String forceStateStoreClassname) {
            this.forceStateStoreClassname = forceStateStoreClassname;
            return this;
        }

        public Builder createMultiPlatformBuilder(boolean createMultiPlatformBuilder) {
            this.createMultiPlatformBuilder = createMultiPlatformBuilder;
            return this;
        }

        public Builder systemTestStandalonePropertiesTemplate(SystemTestStandaloneProperties standalonePropertiesTemplate) {
            this.standalonePropertiesTemplate = standalonePropertiesTemplate;
            return this;
        }

        public Builder instancePropertiesOverrides(InstanceProperties instancePropertiesOverrides) {
            this.instancePropertiesOverrides = instancePropertiesOverrides;
            return this;
        }

        public Builder loadFromSystemProperties() {
            return shortTestId(System.getProperty("sleeper.system.test.short.id"))
                    .vpcId(System.getProperty("sleeper.system.test.vpc.id"))
                    .subnetIds(System.getProperty("sleeper.system.test.subnet.ids"))
                    .findDirectories()
                    .outputDirectory(getOptionalProperty("sleeper.system.test.output.dir")
                            .map(Path::of)
                            .orElse(null))
                    .systemTestClusterEnabled(getBooleanProperty("sleeper.system.test.cluster.enabled", false))
                    .forceRedeploySystemTest(getBooleanProperty("sleeper.system.test.force.redeploy", false))
                    .forceRedeployInstances(getBooleanProperty("sleeper.system.test.instances.force.redeploy", false))
                    .forceStateStoreClassname(getOptionalProperty("sleeper.system.test.force.statestore.classname").orElse(null))
                    .createMultiPlatformBuilder(getBooleanProperty("sleeper.system.test.create.multi.platform.builder", true))
                    .systemTestStandalonePropertiesTemplate(getOptionalProperty("sleeper.system.test.standalone.properties.template")
                            .map(Paths::get)
                            .map(SystemTestStandaloneProperties::fromFile)
                            .orElseGet(SystemTestStandaloneProperties::new))
                    .instancePropertiesOverrides(getOptionalProperty("sleeper.system.test.instance.properties.overrides")
                            .map(Paths::get)
                            .map(LoadLocalProperties::loadInstancePropertiesNoValidation)
                            .orElseGet(InstanceProperties::new));
        }

        public Builder findDirectories() {
            return scriptsDirectory(findScriptsDir())
                    .pythonDirectory(findPythonDir());
        }

        public SystemTestParameters build() {
            return new SystemTestParameters(this);
        }
    }

    private static boolean getBooleanProperty(String property, boolean defaultValue) {
        return getOptionalProperty(property)
                .map(Boolean::valueOf)
                .orElse(defaultValue);
    }

    private static Optional<String> getOptionalProperty(String property) {
        return Optional.ofNullable(System.getProperty(property))
                .filter(not(String::isEmpty));
    }
}
