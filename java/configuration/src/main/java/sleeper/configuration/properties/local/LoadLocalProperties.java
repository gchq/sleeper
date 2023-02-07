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
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SleeperProperties.loadProperties;

public class LoadLocalProperties {

    private LoadLocalProperties() {
    }

    public static <T extends InstanceProperties> T loadInstanceProperties(T properties, Path file) throws IOException {
        properties.load(file);
        Path tagsFile = directoryOf(file).resolve("tags.properties");
        if (Files.exists(tagsFile)) {
            try (Reader reader = Files.newBufferedReader(tagsFile)) {
                properties.loadTags(reader);
            }
        }
        return properties;
    }

    public static Stream<TableProperties> loadTablesFromPath(
            InstanceProperties instanceProperties, Path instancePropertiesFile) {
        Path baseDir = directoryOf(instancePropertiesFile);
        return streamBaseAndTableFolders(baseDir)
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
                Schema schema = Schema.load(schemaPath);
                return new TableProperties(instanceProperties, schema, properties);
            }
            return new TableProperties(instanceProperties, properties);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path directoryOf(Path filePath) {
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
