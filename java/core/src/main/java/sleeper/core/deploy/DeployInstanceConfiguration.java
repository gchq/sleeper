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

package sleeper.core.deploy;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The configuration to deploy a Sleeper instance and configure Sleeper tables.
 */
public class DeployInstanceConfiguration {
    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tableProperties;

    private DeployInstanceConfiguration(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
    }

    public DeployInstanceConfiguration(InstanceProperties instanceProperties, TableProperties tableProperties) {
        this(builder().instanceProperties(instanceProperties).tableProperties(tableProperties));
    }

    public DeployInstanceConfiguration(InstanceProperties instanceProperties, List<TableProperties> tableProperties) {
        this(builder().instanceProperties(instanceProperties).tableProperties(tableProperties));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an instance configuration from local files.
     *
     * @param  instancePropertiesPath the path to the local configuration instance properties file
     * @return                        the instance configuration
     */
    public static DeployInstanceConfiguration fromLocalConfiguration(Path instancePropertiesPath) {
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesNoValidation(instancePropertiesPath);
        List<TableProperties> tableProperties = LoadLocalProperties
                .loadTablesFromInstancePropertiesFileNoValidation(instanceProperties, instancePropertiesPath)
                .collect(Collectors.toUnmodifiableList());
        return DeployInstanceConfiguration.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties).build();
    }

    /**
     * Creates a configuration for a new instance, setting tables from templates if not specified.
     *
     * @param  instancePropertiesPath     the path to the local configuration instance properties file
     * @param  populateInstanceProperties the settings to populate the instance properties
     * @param  fromTemplates              the settings to load the templates
     * @return                            the instance configuration
     */
    public static DeployInstanceConfiguration forNewInstanceDefaultingTables(
            Path instancePropertiesPath, PopulateInstanceProperties populateInstanceProperties,
            DeployInstanceConfigurationFromTemplates fromTemplates) {
        DeployInstanceConfiguration configuration = fromLocalConfiguration(instancePropertiesPath);
        populateInstanceProperties.populate(configuration.getInstanceProperties());
        if (configuration.getTableProperties().isEmpty()) {
            configuration = configuration.withTableProperties(instanceProperties -> List.of(
                    fromTemplates.loadTableProperties(instanceProperties)));
        }
        return configuration;
    }

    /**
     * Creates a configuration for a new instance, setting instance properties from templates if not
     * specified.
     *
     * @param  instancePropertiesPath     the path to the local configuration instance properties file, or null if not
     *                                    present
     * @param  populateInstanceProperties the settings to populate the instance properties
     * @param  templatesDir               the directory to load the templates from
     * @return                            the instance configuration
     */
    public static DeployInstanceConfiguration forNewInstanceDefaultingInstance(
            Path instancePropertiesPath, PopulateInstanceProperties populateInstanceProperties,
            Path templatesDir) {
        if (instancePropertiesPath != null) {
            return fromLocalConfiguration(instancePropertiesPath);
        } else {
            return new DeployInstanceConfiguration(DeployInstanceConfigurationFromTemplates.loadInstanceProperties(templatesDir), List.of());
        }
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public List<TableProperties> getTableProperties() {
        return tableProperties;
    }

    /**
     * Creates an instance configuration with the same instance properties but the given table properties. Takes a
     * function to build the table properties from the instance properties. Note that the same instance properties
     * object is reused, and any changes to those will impact both the before and after instance configuration.
     *
     * @param  buildTableProperties the function to build the table properties
     * @return                      the new instance configuration
     */
    public DeployInstanceConfiguration withTableProperties(Function<InstanceProperties, List<TableProperties>> buildTableProperties) {
        return new DeployInstanceConfiguration(instanceProperties, buildTableProperties.apply(instanceProperties));
    }

    /**
     * Gets the table properties. Fails if there is not exactly one table in the configuration.
     *
     * @return the table properties
     */
    public TableProperties singleTableProperties() {
        if (tableProperties.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one table in configuration");
        }
        return tableProperties.get(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeployInstanceConfiguration that = (DeployInstanceConfiguration) o;
        return Objects.equals(instanceProperties, that.instanceProperties) && Objects.equals(tableProperties, that.tableProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceProperties, tableProperties);
    }

    @Override
    public String toString() {
        return "DeployInstanceConfiguration{" +
                "instanceProperties=" + instanceProperties +
                ", tableProperties=" + tableProperties +
                '}';
    }

    /**
     * A builder for instances of this class.
     */
    public static final class Builder {
        private InstanceProperties instanceProperties;
        private List<TableProperties> tableProperties;

        public Builder() {
        }

        /**
         * Sets the instance properties.
         *
         * @param  instanceProperties the instance properties.
         * @return                    this builder
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Sets the table properties for all tables in this instance.
         *
         * @param  tableProperties the table properties.
         * @return                 this builder
         */
        public Builder tableProperties(List<TableProperties> tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        /**
         * Sets the table properties for an instance with only one table.
         *
         * @param  tableProperties the table properties.
         * @return                 this builder
         */
        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = List.of(tableProperties);
            return this;
        }

        public DeployInstanceConfiguration build() {
            return new DeployInstanceConfiguration(this);
        }
    }
}
