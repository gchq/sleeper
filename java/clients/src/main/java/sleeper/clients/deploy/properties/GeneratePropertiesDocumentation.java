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
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertyGroup;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
                writeFile(instancePath.resolve(groupNameToFileName(instancePropertyGroup)),
                        stream -> writePropertiesMarkdownFile(new InstanceProperties(), instancePropertyGroup, stream));
            } catch (IOException e) {
                System.out.println("Unable to write property file for group: " + instancePropertyGroup.getName());
            }
        });

        //---------- Table Properties ----------
        Path tablePath = Files.createDirectories(headPath.resolve("table/"));
        TablePropertyGroup.getAll().forEach(tablePropertyGroup -> {
            try {
                writeFile(tablePath.resolve(groupNameToFileName(tablePropertyGroup)),
                        stream -> writePropertiesMarkdownFile(new TableProperties(new InstanceProperties()), tablePropertyGroup, stream));
            } catch (IOException e) {
                System.out.println("Unable to write property file for group: " + tablePropertyGroup.getName());
            }
        });
    }

    /**
     * Generates table containing all the properties for a given property group in markdown format.
     * This is then written out to a file named for the property group.
     *
     * @param group the group of properties
     * @param out   the stream for the output
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void writePropertiesMarkdownFile(SleeperProperties properties, PropertyGroup group, OutputStream out) {
        PrintStream stream = new PrintStream(out);
        stream.println("## " + group.getName().toUpperCase());
        stream.println();
        stream.println("Below is a table containing all the details for the property group: " + group.getName());
        stream.println();
        TableWriter tableWriter = TableWriterPropertyHelper.generateTableBuildForGroup(properties.getPropertiesIndex().getAllInGroup(group).stream());
        tableWriter.write(stream);
    }

    private static void writeFile(Path file, Consumer<OutputStream> generator) throws IOException {
        try (OutputStream stream = Files.newOutputStream(file)) {
            generator.accept(stream);
        }
    }

    private static String groupNameToFileName(PropertyGroup group) {
        return group.getName().toLowerCase().replace(" ", "_") + ".md";
    }
}
