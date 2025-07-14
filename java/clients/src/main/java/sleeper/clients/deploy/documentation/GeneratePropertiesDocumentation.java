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
package sleeper.clients.deploy.documentation;

import org.apache.commons.io.file.PathUtils;

import sleeper.clients.deploy.properties.SleeperPropertyMarkdownTable;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.instance.UserDefinedInstanceProperty;
import sleeper.core.properties.table.TableProperty;
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
        InstancePropertyGroup.getAll().forEach(group -> {
            List<UserDefinedInstanceProperty> userDefined = UserDefinedInstanceProperty.getAllInGroup(group);
            List<CdkDefinedInstanceProperty> cdkDefined = CdkDefinedInstanceProperty.getAllInGroup(group);
            if (!userDefined.isEmpty()) {
                writeFile(instanceUserPath.resolve(groupFileName(group)),
                        output -> writePropertiesMarkdownFile(output, "Instance Properties - ", group, " - User Defined", true,
                                SleeperPropertyMarkdownTable.createTableWriterForUserDefinedProperties(userDefined)));
            }
            if (!cdkDefined.isEmpty()) {
                writeFile(instanceCdkPath.resolve(groupFileName(group)),
                        output -> writePropertiesMarkdownFile(output, "Instance Properties - ", group, " - CDK Defined", false,
                                SleeperPropertyMarkdownTable.createTableWriterForCdkDefinedProperties(cdkDefined)));
            }
        });

        //---------- Table Properties ----------
        Path tablePath = Files.createDirectories(headPath.resolve("table/"));
        TablePropertyGroup.getAll().forEach(group -> {
            writeFile(tablePath.resolve(groupFileName(group)),
                    output -> writePropertiesMarkdownFile(output, "Table Properties - ", group, "", true,
                            SleeperPropertyMarkdownTable.createTableWriterForTableProperties(
                                    TableProperty.getAllInGroup(group))));
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
            if (!UserDefinedInstanceProperty.getAllInGroup(group).isEmpty()) {
                out.println(groupPageLink("instance/user/", group));
            }
        });
        out.println();
        out.println("### CDK defined");
        out.println();
        out.println("Below you can find all properties that can be set for a Sleeper instance by the CDK during deployment.");
        out.println();
        instanceGroups.forEach(group -> {
            if (!CdkDefinedInstanceProperty.getAllInGroup(group).isEmpty()) {
                out.println(groupPageLink("instance/cdk/", group));
            }
        });
        out.println();
        out.println("## Table Properties");
        out.println();
        out.println("Below you can find all properties that can be set for a Sleeper table.");
        out.println();
        tableGroups.forEach(group -> {
            out.println(groupPageLink("table/", group));
        });
    }

    private static String groupPageLink(String directory, PropertyGroup group) {
        return String.format("[%s](properties/%s%s)<br>", group.getName(), directory, groupFileName(group));
    }

    private static <T extends SleeperProperty> void writePropertiesMarkdownFile(
            OutputStream output, String groupNamePrefix, PropertyGroup group, String groupNameSuffix, boolean writeDetails, TableWriter tableWriter) {
        PrintStream out = printStream(output);
        out.println("## " + groupNamePrefix + group.getName() + groupNameSuffix);
        out.println();
        out.println(adjustLongEntryForMarkdown(group.getDescription()));
        out.println();
        if (writeDetails && group.getDetails() != null) {
            out.println(adjustLongEntryForMarkdown(group.getDetails()));
            out.println();
        }
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

    private static String groupFileName(PropertyGroup group) {
        return group.getName().toLowerCase(Locale.ENGLISH).replace(" ", "_") + ".md";
    }

    private static List<PropertyGroup> sortByName(List<PropertyGroup> groups) {
        return groups.stream()
                .sorted(Comparator.comparing(PropertyGroup::getName))
                .toList();
    }

    private static String adjustLongEntryForMarkdown(String valueIn) {
        return valueIn.replaceAll("\n", "<br>");
    }
}
