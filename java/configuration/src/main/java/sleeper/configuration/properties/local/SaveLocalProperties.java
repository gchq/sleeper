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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Saves Sleeper configuration files to the local file system.
 */
public class SaveLocalProperties {

    private SaveLocalProperties() {
    }

    /**
     * Saves instance and table properties to the given directory.
     *
     * @param  directory             the directory
     * @param  instanceProperties    the instance properties
     * @param  tablePropertiesStream the table properties
     * @throws IOException           if we could not write to the file system
     */
    public static void saveToDirectory(
            Path directory, InstanceProperties instanceProperties,
            Stream<TableProperties> tablePropertiesStream) throws IOException {
        writeInstanceProperties(instanceProperties, directory.resolve("instance.properties"));
        Files.writeString(directory.resolve("tags.properties"), instanceProperties.getTagsPropertiesAsString());
        saveTablesToDirectory(directory, tablePropertiesStream);
    }

    private static void saveTablesToDirectory(
            Path directory, Stream<TableProperties> tablePropertiesStream) throws IOException {
        try {
            for (TableProperties tableProperties : (Iterable<TableProperties>) tablePropertiesStream::iterator) {
                saveTableToDirectory(directory, tableProperties);
            }
        } catch (UncheckedIOException e) {
            // Stream could throw an UncheckedIOException, so unwrap it
            throw e.getCause();
        }
    }

    private static void saveTableToDirectory(Path directory, TableProperties tableProperties) throws IOException {
        // Store in the same directory structure as in S3 (tables/table-name)
        Path tableDir = directory.resolve("tables").resolve(tableProperties.get(TABLE_NAME));
        Files.createDirectories(tableDir);
        writeTableProperties(tableProperties, tableDir.resolve("table.properties"));

        // Write schema
        tableProperties.getSchema().save(tableDir.resolve("schema.json"));
    }

    private static void writeInstanceProperties(InstanceProperties instanceProperties, Path file) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            InstanceProperties.createPrettyPrinter(new PrintWriter(writer))
                    .print(instanceProperties);
        }
    }

    private static void writeTableProperties(TableProperties tableProperties, Path file) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            TableProperties.createPrettyPrinter(new PrintWriter(writer))
                    .print(tableProperties);
        }
    }
}
