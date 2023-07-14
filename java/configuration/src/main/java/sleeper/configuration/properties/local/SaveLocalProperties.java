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

import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.configuration.properties.table.TableProperties;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SaveLocalProperties {

    private SaveLocalProperties() {
    }

    public static InstanceProperties saveFromS3(AmazonS3 s3, String instanceId, Path directory) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        saveToDirectory(directory, instanceProperties, TableProperties.streamTablesFromS3(s3, instanceProperties));
        return instanceProperties;
    }

    public static void saveToDirectory(Path directory,
                                       InstanceProperties instanceProperties,
                                       Stream<TableProperties> tablePropertiesStream) throws IOException {
        writeInstanceProperties(instanceProperties, directory.resolve("instance.properties"));
        Files.writeString(directory.resolve("tags.properties"), instanceProperties.getTagsPropertiesAsString());
        writeStringIfSet(directory.resolve("configBucket.txt"), instanceProperties.get(CONFIG_BUCKET));
        writeStringIfSet(directory.resolve("queryResultsBucket.txt"), instanceProperties.get(QUERY_RESULTS_BUCKET));
        saveTablesToDirectory(directory, tablePropertiesStream);
    }

    private static void saveTablesToDirectory(Path directory, Stream<TableProperties> tablePropertiesStream) throws IOException {
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

        // Unpack properties for schema & table bucket
        tableProperties.getSchema().save(tableDir.resolve("schema.json"));
        writeStringIfSet(tableDir.resolve("tableBucket.txt"), tableProperties.get(DATA_BUCKET));
    }

    private static void writeStringIfSet(Path file, String value) throws IOException {
        if (value != null) {
            Files.writeString(file, value);
        }
    }

    private static void writeInstanceProperties(InstanceProperties instanceProperties, Path file) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            SleeperPropertiesPrettyPrinter.forInstanceProperties(new PrintWriter(writer))
                    .print(instanceProperties);
        }
    }

    private static void writeTableProperties(TableProperties tableProperties, Path file) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            SleeperPropertiesPrettyPrinter.forTableProperties(new PrintWriter(writer))
                    .print(tableProperties);
        }
    }
}
