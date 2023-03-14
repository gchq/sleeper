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
package sleeper.configuration.properties.local;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

public class LoadLocalProperties {

    private LoadLocalProperties() {
    }

    public static InstanceProperties loadInstancePropertiesFromDirectory(Path directory) {
        return loadInstancePropertiesFromDirectory(new InstanceProperties(), directory);
    }

    public static <T extends InstanceProperties> T loadInstancePropertiesFromDirectory(
            T instanceProperties, Path directory) {
        return loadInstanceProperties(instanceProperties, directory.resolve("instance.properties"));
    }

    public static <T extends InstanceProperties> T loadInstanceProperties(T properties, Path file) {
        try {
            properties.load(file);
            Path tagsFile = directoryOf(file).resolve("tags.properties");
            if (Files.exists(tagsFile)) {
                Properties tagsProperties = loadProperties(tagsFile);
                properties.loadTags(tagsProperties);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return properties;
    }

    public static Stream<TableProperties> loadTablesFromInstancePropertiesFile(
            InstanceProperties instanceProperties, Path instancePropertiesFile) {
        return loadTablesFromDirectory(instanceProperties, directoryOf(instancePropertiesFile));
    }

    public static Stream<TableProperties> loadTablesFromDirectory(
            InstanceProperties instanceProperties, Path directory) {
        return streamBaseAndTableFolders(directory)
                .map(folder -> readTablePropertiesFolderOrNull(instanceProperties, folder))
                .filter(Objects::nonNull);
    }

    private static TableProperties readTablePropertiesFolderOrNull(
            InstanceProperties instanceProperties, Path folder) {
        Path propertiesPath = folder.resolve("table.properties");
        Path schemaPath = folder.resolve("schema.json");
        if (!Files.exists(propertiesPath)) {
            return null;
        }
        try {
            Properties properties = loadProperties(propertiesPath);
            if (Files.exists(schemaPath)) {
                String schemaString = Files.readString(schemaPath);
                properties.setProperty(TableProperty.SCHEMA.getPropertyName(), schemaString);
            }
            return TableProperties.loadAndValidate(instanceProperties, properties);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Path directoryOf(Path filePath) {
        Path parent = filePath.getParent();
        if (parent == null) {
            return Paths.get(".");
        } else {
            return parent;
        }
    }

    private static Stream<Path> streamBaseAndTableFolders(Path baseDir) {
        return Stream.concat(
                Stream.of(baseDir),
                streamTableFolders(baseDir));
    }

    private static Stream<Path> streamTableFolders(Path baseDir) {
        Path tablesFolder = baseDir.resolve("tables");
        if (!Files.isDirectory(tablesFolder)) {
            return Stream.empty();
        }
        List<Path> tables;
        try (Stream<Path> pathStream = Files.list(tablesFolder)) {
            tables = pathStream
                    .filter(Files::isDirectory)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list table configuration directories", e);
        }
        return tables.stream().sorted();
    }
}
