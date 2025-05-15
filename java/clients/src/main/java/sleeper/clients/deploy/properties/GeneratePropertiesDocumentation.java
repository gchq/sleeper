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

import org.apache.commons.io.file.PathUtils;

import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertyGroup;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

public class GeneratePropertiesDocumentation {

    private GeneratePropertiesDocumentation() {
    }

    public static void generateDocumentation(Path root) throws IOException {
        generateMasterPage(root);
        Path headPath = root.resolve("docs/usage/properties");
        if (Files.isDirectory(headPath)) {
            PathUtils.deleteDirectory(headPath);
        }
        Files.createDirectories(headPath);

        //---------- Instance Properties ----------
        Path instanceUserPath = Files.createDirectories(headPath.resolve("instance/user/"));
        Path instanceCdkPath = Files.createDirectories(headPath.resolve("instance/cdk/"));
        InstancePropertyGroup.getAll().forEach(instancePropertyGroup -> {
            writeFile(instanceUserPath.resolve(groupNameToFileName(instancePropertyGroup.getName())),
                    output -> writePropertiesMarkdownFile(new InstanceProperties(), instancePropertyGroup, output));
            writeFile(instanceCdkPath.resolve(groupNameToFileName(instancePropertyGroup.getName())),
                    output -> writePropertiesMarkdownFile(new InstanceProperties(), instancePropertyGroup, output));
        });

        //---------- Table Properties ----------
        Path tablePath = Files.createDirectories(headPath.resolve("table/"));
        TablePropertyGroup.getAll().forEach(tablePropertyGroup -> {
            writeFile(tablePath.resolve(groupNameToFileName(tablePropertyGroup.getName())),
                    output -> writePropertiesMarkdownFile(new TableProperties(new InstanceProperties()), tablePropertyGroup, output));
        });
    }

    private static void generateMasterPage(Path root) throws IOException {
        writeFile(root.resolve("docs/usage/property-master.md"), GeneratePropertiesDocumentation::generateDocumentLinks);
    }

    private static void generateDocumentLinks(OutputStream output) {
        List<PropertyGroup> instanceGroups = sortByName(InstancePropertyGroup.getAll());
        List<PropertyGroup> tableGroups = sortByName(TablePropertyGroup.getAll());
        PrintStream out = printStream(output);
        out.println("## Instance Properties");
        out.println();
        out.println("### User defined");
        out.println();
        out.println("Below you can find all properties that can be set for a Sleeper instance by the user.");
        out.println();
        instanceGroups.forEach(group -> {
            out.println(pageLinkFromGroupName(group.getName(), "instance/user/"));
        });
        out.println();
        out.println("### CDK defined");
        out.println();
        out.println("Below you can find all properties that can be set for a Sleeper instance by the CDK.");
        out.println();
        instanceGroups.forEach(group -> {
            out.println(pageLinkFromGroupName(group.getName(), "instance/cdk/"));
        });
        out.println();
        out.println("## Table Properties");
        out.println();
        out.println("Below you can find all properties that can be set for a Sleeper table.");
        out.println();
        tableGroups.forEach(group -> {
            out.println(pageLinkFromGroupName(group.getName(), "table/"));
        });
    }

    private static String pageLinkFromGroupName(String group, String directory) {
        return String.format("[%s](properties/%s%s)<br>", group, directory, groupNameToFileName(group));
    }

    /**
     * Generates table containing all the properties for a given property group in markdown format.
     * This is then written out to a file named for the property group.
     *
     * @param  output                       the stream for the output
     * @throws UnsupportedEncodingException thrown if uft8 unavailable
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void writePropertiesMarkdownFile(SleeperProperties properties, PropertyGroup group, OutputStream output) {
        PrintStream out = printStream(output);
        out.println("## " + group.getName().toUpperCase(Locale.ENGLISH));
        out.println();
        out.println("Below is a table containing all the details for the property group: " + group.getName());
        out.println();
        TableWriter tableWriter = SleeperPropertyMarkdownTable.generateTableBuildForGroup(properties.getPropertiesIndex().getAllInGroup(group).stream());
        tableWriter.write(out);
    }

    private static PrintStream printStream(OutputStream output) {
        try {
            return new PrintStream(output, true, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeFile(Path file, Consumer<OutputStream> generator) {
        try (OutputStream stream = Files.newOutputStream(file)) {
            generator.accept(stream);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed writing file: " + file, e);
        }
    }

    private static String groupNameToFileName(String group) {
        return group.toLowerCase(Locale.ENGLISH).replace(" ", "_") + ".md";
    }

    private static List<PropertyGroup> sortByName(List<PropertyGroup> groups) {
        return groups.stream()
                .sorted(Comparator.comparing(PropertyGroup::getName))
                .toList();
    }
}
