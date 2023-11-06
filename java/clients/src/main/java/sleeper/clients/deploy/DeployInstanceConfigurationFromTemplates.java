/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.clients.deploy;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

public class DeployInstanceConfigurationFromTemplates {
    private final Path instancePropertiesPath;
    private final Path templatesDir;

    private DeployInstanceConfigurationFromTemplates(Builder builder) {
        instancePropertiesPath = builder.instancePropertiesPath;
        templatesDir = builder.templatesDir;
    }

    public static Builder builder() {
        return new Builder();
    }

    public DeployInstanceConfiguration load() throws IOException {
        if (instancePropertiesPath == null) {
            return fromTemplateDirectory(templatesDir);
        }
        Path rootDir = instancePropertiesPath.getParent();
        if (rootDir == null) {
            throw new IllegalArgumentException("Could not find parent of instance properties file");
        }
        InstanceProperties instanceProperties = new InstanceProperties(
                loadProperties(instancePropertiesPath));
        if (Files.exists(rootDir.resolve("tags.properties"))) {
            instanceProperties.loadTags(loadProperties(rootDir.resolve("tags.properties")));
        } else {
            instanceProperties.loadTags(loadProperties(templatesDir.resolve("tags.template")));
        }
        Properties properties;
        if (Files.exists(rootDir.resolve("table.properties"))) {
            properties = loadProperties(rootDir.resolve("table.properties"));
        } else {
            properties = loadProperties(templatesDir.resolve("tableproperties.template"));
        }
        if (Files.exists(rootDir.resolve("schema.json"))) {
            properties.setProperty(TableProperty.SCHEMA.getPropertyName(),
                    Files.readString(rootDir.resolve("schema.json")));
        } else {
            properties.setProperty(TableProperty.SCHEMA.getPropertyName(),
                    Files.readString(templatesDir.resolve("schema.template")));
        }
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        return DeployInstanceConfiguration.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties).build();
    }

    public static DeployInstanceConfiguration fromTemplateDirectory(Path templatesDir) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties(
                loadProperties(templatesDir.resolve("instanceproperties.template")));
        instanceProperties.loadTags(loadProperties(templatesDir.resolve("tags.template")));
        Properties properties = loadProperties(templatesDir.resolve("tableproperties.template"));
        properties.setProperty(TableProperty.SCHEMA.getPropertyName(),
                Files.readString(templatesDir.resolve("schema.template")));
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        return DeployInstanceConfiguration.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties).build();
    }

    public static DeployInstanceConfiguration fromInstancePropertiesOrTemplatesDir(Path instancePropertiesPath, Path templatesDir) throws IOException {
        return builder().instancePropertiesPath(instancePropertiesPath).templatesDir(templatesDir).build().load();
    }

    public static final class Builder {
        private Path instancePropertiesPath;
        private Path templatesDir;

        private Builder() {
        }

        public Builder instancePropertiesPath(Path instancePropertiesPath) {
            this.instancePropertiesPath = instancePropertiesPath;
            return this;
        }

        public Builder templatesDir(Path templatesDir) {
            this.templatesDir = templatesDir;
            return this;
        }

        public DeployInstanceConfigurationFromTemplates build() {
            return new DeployInstanceConfigurationFromTemplates(this);
        }
    }
}
