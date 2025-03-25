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
package sleeper.clients.deploy.properties;

import sleeper.clients.util.table.TableWriter;
import sleeper.clients.util.table.TableWriterPropertyHelper;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertyGroup;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

public class GeneratePropertiesDocumentation {

    private GeneratePropertiesDocumentation() {
    }

    public static void generateDocumentation(Path root) throws IOException {
        Path headPath = Files.createDirectories(root.resolve("docs/usage/properties"));

        //---------- Instance Properties ----------
        Path instancePath = Files.createDirectories(headPath.resolve("instance/"));
        InstancePropertyGroup.getAll().forEach(instancePropertyGroup -> {
            try {
                writeFile(instancePath.resolve(instancePropertyGroup.getName().toLowerCase().replace(" ", "_") + ".md"),
                        stream -> writeInstancePropertiesMarkdownTable(instancePropertyGroup, stream));
            } catch (IOException e) {
                System.out.println("Unable to write property file for group: " + instancePropertyGroup.getName());
            }
        });

        //---------- Table Properties ----------
        Path tablePath = Files.createDirectories(headPath.resolve("table/"));
        TablePropertyGroup.getAll().forEach(tablePropertyGroup -> {
            try {
                writeFile(tablePath.resolve(tablePropertyGroup.getName().toLowerCase().replace(" ", "_") + ".md"),
                        stream -> writeTablePropertiesMarkdownTable(tablePropertyGroup, stream));
            } catch (IOException e) {
                System.out.println("Unable to write property file for group: " + tablePropertyGroup.getName());
            }
        });
    }

    /**
     * Generates table containing all the instance properties for a given property group in markdown format.
     * This is then written out to a named file.
     *
     * @param group the group of properties
     * @param out   the writer for the files
     */
    private static void writeInstancePropertiesMarkdownTable(PropertyGroup group, OutputStream out) {
        PrintStream stream = new PrintStream(out);
        InstanceProperties properties = new InstanceProperties();
        stream.println(group.getName().toUpperCase());
        stream.println();
        stream.println("Below is a table containing all the details relevant for the instance properties within the " + group.getName() + " group.");
        stream.println();
        TableWriter tableWriter = TableWriterPropertyHelper.generateTableBuildForGroup(properties.getPropertiesIndex().getAllInGroup(group).stream());
        tableWriter.write(stream);
    }

    /**
     * Generates table containing all the table properties for a given property group in markdown format.
     * This is then written out to a named file.
     *
     * @param group the group of properties
     * @param out   the writer for the file
     */
    private static void writeTablePropertiesMarkdownTable(PropertyGroup group, OutputStream out) {
        PrintStream stream = new PrintStream(out);
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        stream.println(group.getName().toUpperCase());
        stream.println();
        stream.println("Below is a table containing all the details relevant for the table properties within the " + group.getName() + " group.");
        stream.println();
        TableWriter tableWriter = TableWriterPropertyHelper.generateTableBuildForGroupTable(tableProperties.getPropertiesIndex().getAllInGroup(group).stream());
        tableWriter.write(stream);
    }

    private static void writeFile(Path file, Consumer<OutputStream> generator) throws IOException {
        try (OutputStream stream = Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            generator.accept(stream);
        }
    }
}
