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
package sleeper.configuration.properties.local;

import sleeper.configuration.properties.instance.InstanceProperties;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

/**
 * Loads Sleeper configuration files from the local file system.
 */
public class LoadLocalProperties {

    private LoadLocalProperties() {
    }

    /**
     * Loads and validates instance properties from a given directory. Looks for an instance properties file and a tags
     * file.
     *
     * @param  directory the directory
     * @return           the instance properties
     */
    public static InstanceProperties loadInstancePropertiesFromDirectory(Path directory) {
        Path file = directory.resolve("instance.properties");
        return loadInstanceProperties(InstanceProperties::createWithoutValidation, file);
    }

    /**
     * Loads and validates instance properties from a given instance properties file. Also loads a tags file if present.
     *
     * @param  file the instance properties file
     * @return      the instance properties
     */
    public static InstanceProperties loadInstanceProperties(Path file) {
        return loadInstanceProperties(InstanceProperties::createWithoutValidation, file);
    }

    /**
     * Loads instance properties from a given instance properties file, with no validation. Also loads a tags file if
     * present.
     *
     * @param  file the instance properties file
     * @return      the instance properties
     */
    public static InstanceProperties loadInstancePropertiesNoValidation(Path file) {
        return loadInstancePropertiesNoValidation(InstanceProperties::createWithoutValidation, file);
    }

    private static <T extends InstanceProperties> T loadInstanceProperties(Function<Properties, T> constructor, Path file) {
        T properties = loadInstancePropertiesNoValidation(constructor, file);
        properties.validate();
        return properties;
    }

    /**
     * Loads instance properties from a given instance properties file, with no validation. Also loads a tags file if
     * present. This should only be used with a class that extends {@link InstanceProperties}, to create an instance
     * of that class.
     *
     * @param  constructor a method to create instance properties from the given properties object
     * @param  file        the instance properties file
     * @return             the instance properties
     */
    public static <T extends InstanceProperties> T loadInstancePropertiesNoValidation(Function<Properties, T> constructor, Path file) {
        T properties = constructor.apply(loadProperties(file));
        Path tagsFile = directoryOf(file).resolve("tags.properties");
        if (Files.exists(tagsFile)) {
            Properties tagsProperties = loadProperties(tagsFile);
            properties.loadTags(tagsProperties);
        }
        return properties;
    }

    /**
     * Loads and validates table properties by scanning alongside the given instance properties file.
     *
     * @param  instanceProperties     the instance properties
     * @param  instancePropertiesFile the path to the instance properties file
     * @return                        the table properties found
     */
    public static Stream<TableProperties> loadTablesFromInstancePropertiesFile(
            InstanceProperties instanceProperties, Path instancePropertiesFile) {
        return loadTablesFromDirectory(instanceProperties, directoryOf(instancePropertiesFile));
    }

    /**
     * Loads table properties by scanning alongside the given instance properties file, with no validation.
     *
     * @param  instanceProperties     the instance properties
     * @param  instancePropertiesFile the path to the instance properties file
     * @return                        the table properties found
     */
    public static Stream<TableProperties> loadTablesFromInstancePropertiesFileNoValidation(
            InstanceProperties instanceProperties, Path instancePropertiesFile) {
        return loadTablesFromDirectoryNoValidation(instanceProperties, directoryOf(instancePropertiesFile));
    }

    /**
     * Loads and validates table properties by scanning under the given directory.
     *
     * @param  instanceProperties the instance properties
     * @param  directory          the directory
     * @return                    the table properties found
     */
    public static Stream<TableProperties> loadTablesFromDirectory(
            InstanceProperties instanceProperties, Path directory) {
        return loadTablesFromDirectoryNoValidation(instanceProperties, directory)
                .map(tableProperties -> {
                    tableProperties.validate();
                    return tableProperties;
                });
    }

    /**
     * Loads table properties by scanning under the given directory, with no validation.
     *
     * @param  instanceProperties the instance properties
     * @param  directory          the directory
     * @return                    the table properties found
     */
    public static Stream<TableProperties> loadTablesFromDirectoryNoValidation(
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
            return new TableProperties(instanceProperties, properties);
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
