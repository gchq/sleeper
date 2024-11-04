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

import sleeper.core.properties.PropertiesUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Settings to create a configuration for a Sleeper instance from templates.
 */
public class DeployInstanceConfigurationFromTemplates {
    private final Path templatesDir;
    private final String tableNameForTemplate;
    private final Path splitPointsFileForTemplate;

    private DeployInstanceConfigurationFromTemplates(Builder builder) {
        templatesDir = builder.templatesDir;
        tableNameForTemplate = builder.tableNameForTemplate;
        splitPointsFileForTemplate = builder.splitPointsFileForTemplate;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Loads the templates for the instance and table properties.
     *
     * @return the configuration
     */
    public DeployInstanceConfiguration load() {
        InstanceProperties instanceProperties = loadInstanceProperties(templatesDir);
        return DeployInstanceConfiguration.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(loadTableProperties(instanceProperties))
                .build();
    }

    /**
     * Loads the templates for the table properties.
     *
     * @param  instanceProperties the instance properties
     * @return                    the table properties
     */
    public TableProperties loadTableProperties(InstanceProperties instanceProperties) {
        TableProperties tableProperties = loadTableProperties(templatesDir, instanceProperties);
        tableProperties.set(TABLE_NAME, tableNameForTemplate);
        if (splitPointsFileForTemplate != null) {
            if (!Files.exists(splitPointsFileForTemplate)) {
                throw new IllegalArgumentException("Split points file not found: " + splitPointsFileForTemplate);
            }
            tableProperties.set(SPLIT_POINTS_FILE, splitPointsFileForTemplate.toString());
        }
        return tableProperties;
    }

    /**
     * Loads the templates for the instance properties.
     *
     * @param  templatesDir the templates directory
     * @return              the instance properties
     */
    public static InstanceProperties loadInstanceProperties(Path templatesDir) {
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(
                PropertiesUtils.loadProperties(templatesDir.resolve("instanceproperties.template")));
        instanceProperties.loadTags(PropertiesUtils.loadProperties(templatesDir.resolve("tags.template")));
        return instanceProperties;
    }

    private static TableProperties loadTableProperties(Path templatesDir, InstanceProperties instanceProperties) {
        Properties properties = PropertiesUtils.loadProperties(templatesDir.resolve("tableproperties.template"));
        properties.setProperty(TableProperty.SCHEMA.getPropertyName(), loadSchemaJsonTemplate(templatesDir));
        return new TableProperties(instanceProperties, properties);
    }

    private static String loadSchemaJsonTemplate(Path templatesDir) {
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
        private Path templatesDir;
        private String tableNameForTemplate;
        private Path splitPointsFileForTemplate;

        private Builder() {
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
