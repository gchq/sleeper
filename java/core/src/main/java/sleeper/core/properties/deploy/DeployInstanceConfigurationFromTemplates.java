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

package sleeper.core.properties.deploy;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Settings to create a configuration for a Sleeper instance by combining templates with configuration files.
 */
public class DeployInstanceConfigurationFromTemplates {
    private final Path instancePropertiesPath;
    private final Path templatesDir;
    private final String tableNameForTemplate;
    private final Path splitPointsFileForTemplate;

    private DeployInstanceConfigurationFromTemplates(Builder builder) {
        instancePropertiesPath = builder.instancePropertiesPath;
        templatesDir = builder.templatesDir;
        tableNameForTemplate = builder.tableNameForTemplate;
        splitPointsFileForTemplate = builder.splitPointsFileForTemplate;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Load the configuration files, and use templates for any missing components of the configuration.
     *
     * @return the configuration
     */
    public DeployInstanceConfiguration load() {
        if (instancePropertiesPath == null) {
            return fromTemplatesDir();
        }
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesNoValidation(instancePropertiesPath);
        if (instanceProperties.getTags().isEmpty()) {
            loadTagsTemplate(instanceProperties);
        }
        List<TableProperties> tableProperties = LoadLocalProperties
                .loadTablesFromInstancePropertiesFileNoValidation(instanceProperties, instancePropertiesPath)
                .map(properties -> {
                    loadTemplateIfMissing(properties);
                    return properties;
                })
                .collect(Collectors.toUnmodifiableList());
        if (tableProperties.isEmpty()) {
            tableProperties = List.of(loadTablePropertiesTemplate(instanceProperties));
        }
        return DeployInstanceConfiguration.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties).build();
    }

    private DeployInstanceConfiguration fromTemplatesDir() {
        InstanceProperties instanceProperties = loadInstancePropertiesTemplate();
        loadTagsTemplate(instanceProperties);
        TableProperties tableProperties = loadTablePropertiesTemplate(instanceProperties);
        return DeployInstanceConfiguration.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties).build();
    }

    private void loadTemplateIfMissing(TableProperties tableProperties) {
        if (tableProperties.getSchema() == null) {
            tableProperties.setSchema(Schema.loadFromString(loadSchemaJsonTemplate()));
        }
    }

    private InstanceProperties loadInstancePropertiesTemplate() {
        return InstanceProperties.createWithoutValidation(
                loadProperties(templatesDir.resolve("instanceproperties.template")));
    }

    private void loadTagsTemplate(InstanceProperties instanceProperties) {
        instanceProperties.loadTags(loadProperties(templatesDir.resolve("tags.template")));
    }

    private TableProperties loadTablePropertiesTemplate(InstanceProperties instanceProperties) {
        Properties properties = loadProperties(templatesDir.resolve("tableproperties.template"));
        properties.setProperty(TableProperty.SCHEMA.getPropertyName(), loadSchemaJsonTemplate());
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        tableProperties.set(TABLE_NAME, tableNameForTemplate);
        if (splitPointsFileForTemplate != null) {
            if (!Files.exists(splitPointsFileForTemplate)) {
                throw new IllegalArgumentException("Split points file not found: " + splitPointsFileForTemplate);
            }
            tableProperties.set(SPLIT_POINTS_FILE, splitPointsFileForTemplate.toString());
        }
        return tableProperties;
    }

    private String loadSchemaJsonTemplate() {
        try {
            return Files.readString(templatesDir.resolve("schema.template"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * A builder for instances of this class.
     */
    public static final class Builder {
        private Path instancePropertiesPath;
        private Path templatesDir;
        private String tableNameForTemplate;
        private Path splitPointsFileForTemplate;

        private Builder() {
        }

        /**
         * Sets the path to load the Sleeper instance configuration from. This should point to the instance properties
         * file. Any other configuration files will be found relative to this. If this is not set, the templates will
         * be used directly.
         *
         * @param  instancePropertiesPath the path to the instance properties file
         * @return                        this builder
         */
        public Builder instancePropertiesPath(Path instancePropertiesPath) {
            this.instancePropertiesPath = instancePropertiesPath;
            return this;
        }

        /**
         * Sets the path to the templates directory. Templates will be found and loaded based on expected filenames
         * under this directory.
         *
         * @param  templatesDir the directory
         * @return              this builder
         */
        public Builder templatesDir(Path templatesDir) {
            this.templatesDir = templatesDir;
            return this;
        }

        /**
         * Sets the default Sleeper table name if none is specified. If no Sleeper table is specified in the
         * configuration files, one will be created from the template with this table name.
         *
         * @param  tableNameForTemplate the table name
         * @return                      this builder
         */
        public Builder tableNameForTemplate(String tableNameForTemplate) {
            this.tableNameForTemplate = tableNameForTemplate;
            return this;
        }

        /**
         * Sets the split points file to use when creating a Sleeper table from the template.
         *
         * @param  splitPointsFileForTemplate the split points file
         * @return                            this builder
         */
        public Builder splitPointsFileForTemplate(Path splitPointsFileForTemplate) {
            this.splitPointsFileForTemplate = splitPointsFileForTemplate;
            return this;
        }

        public DeployInstanceConfigurationFromTemplates build() {
            return new DeployInstanceConfigurationFromTemplates(this);
        }
    }
}
