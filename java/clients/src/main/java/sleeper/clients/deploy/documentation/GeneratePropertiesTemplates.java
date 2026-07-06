/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.SleeperPropertiesPrettyPrinter;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.properties.table.TablePropertyGroup;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

/**
 * Generates template files to be filled in when deploying an instance of Sleeper, or creating tables.
 */
public class GeneratePropertiesTemplates {

    private GeneratePropertiesTemplates() {
    }

    public static void main(String[] args) throws Exception {
        createTemplates(Path.of(args[0]));
        createDocumentation(Path.of(args[0]));
    }

    /**
     * Generates and writes all template files.
     *
     * @param  repositoryRoot the root directory of the Sleeper repository
     * @throws IOException    if any files could not be written
     */
    public static void createTemplates(Path repositoryRoot) throws IOException {

        Path fullExampleDir = Files.createDirectories(repositoryRoot.resolve("example/full"));
        writeFile(fullExampleDir.resolve("instance.properties"),
                GeneratePropertiesTemplates::writeExampleFullInstanceProperties);
        writeFile(fullExampleDir.resolve("table.properties"),
                GeneratePropertiesTemplates::writeExampleFullTableProperties);

        Path basicExampleDir = Files.createDirectories(repositoryRoot.resolve("example/basic"));
        writeFile(basicExampleDir.resolve("instance.properties"),
                GeneratePropertiesTemplates::writeExampleBasicInstanceProperties);
        writeFile(basicExampleDir.resolve("table.properties"),
                GeneratePropertiesTemplates::writeExampleBasicTableProperties);

        Path scriptsTemplateDir = Files.createDirectories(repositoryRoot.resolve("scripts/templates"));
        writeFile(scriptsTemplateDir.resolve("instanceproperties.template"),
                GeneratePropertiesTemplates::writeInstancePropertiesTemplate);
        writeFile(scriptsTemplateDir.resolve("tableproperties.template"),
                GeneratePropertiesTemplates::writeTablePropertiesTemplate);
    }

    private static void createDocumentation(Path path) throws Exception {
        GeneratePropertiesDocumentation.generateDocumentation(path);
    }

    /**
     * Writes the full instance properties example file to the given writer.
     *
     * @param writer the writer
     */
    public static void writeExampleFullInstanceProperties(Writer writer) {
        InstanceProperties properties = new InstanceProperties();

        writeFullPropertiesTemplate(writer, properties, InstancePropertyGroup.getAll());
    }

    /**
     * Writes the full table properties example file to the given writer.
     *
     * @param writer the writer
     */
    public static void writeExampleFullTableProperties(Writer writer) {
        TableProperties properties = new TableProperties(new InstanceProperties());

        writeFullPropertiesTemplate(writer, properties, TablePropertyGroup.getAll());
    }

    /**
     * Writes the basic instance properties example file to the given writer.
     *
     * @param writer the writer
     */
    public static void writeExampleBasicInstanceProperties(Writer writer) {
        writeBasicPropertiesTemplate(writer,
                new InstanceProperties(),
                InstancePropertyGroup.getAll());
    }

    /**
     * Writes the basic table properties example file to the given writer.
     *
     * @param writer the writer
     */
    public static void writeExampleBasicTableProperties(Writer writer) {
        writeBasicPropertiesTemplate(writer,
                new TableProperties(new InstanceProperties()),
                TablePropertyGroup.getAll());
    }

    /**
     * Writes the instance properties template file to the given writer.
     *
     * @param out the writer
     */
    public static void writeInstancePropertiesTemplate(Writer out) {
        InstanceProperties properties = new InstanceProperties();
        List<InstanceProperty> propertiesByIsSet = properties.getPropertiesIndex().getUserDefined().stream().filter(SleeperProperty::isIncludedInTemplate).toList();

        PrintWriter writer = new PrintWriter(out);
        writer.println("#################################################################################");
        writer.println("#                           SLEEPER INSTANCE PROPERTIES                         #");
        writer.println("#################################################################################");
        writer.println();
        SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                propertiesByIsSet, InstancePropertyGroup.getAll(), writer)
                .print(properties);
    }

    /**
     * Writes the table properties template file to the given writer.
     *
     * @param out the writer
     */
    public static void writeTablePropertiesTemplate(Writer out) {
        TableProperties properties = new TableProperties(new InstanceProperties());

        PrintWriter writer = new PrintWriter(out);
        writer.println("#################################################################################");
        writer.println("#                           SLEEPER TABLE PROPERTIES                            #");
        writer.println("#################################################################################");
        writer.println();
        SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                TableProperty.getAll(), TablePropertyGroup.getAll(), writer)
                .print(properties);
    }

    private static <T extends SleeperProperty> void writeFullPropertiesTemplate(
            Writer out, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups) {
        List<T> definitionsForTemplate = properties.getPropertiesIndex().getUserDefined().stream()
                .filter(SleeperProperty::isIncludedInTemplate)
                .filter(not(properties::isSet))
                .toList();
        PrintWriter writer = new PrintWriter(out);
        SleeperPropertiesPrettyPrinter.forPropertiesTemplate(definitionsForTemplate, propertyGroups, writer)
                .print(properties);
    }

    private static <T extends SleeperProperty> void writeBasicPropertiesTemplate(
            Writer writer, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups) {
        writePropertiesTemplate(writer, properties, propertyGroups,
                properties.getPropertiesIndex().getUserDefined().stream()
                        .filter(property -> property.isIncludedInBasicTemplate()));
    }

    private static <T extends SleeperProperty> void writePropertiesTemplate(
            Writer writer,
            SleeperProperties<T> properties,
            List<PropertyGroup> propertyGroups,
            Stream<T> propertyDefinitions) {
        SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                propertyDefinitions.filter(SleeperProperty::isIncludedInTemplate)
                        .collect(Collectors.toList()),
                propertyGroups, new PrintWriter(writer))
                .print(properties);
    }

    private static void writeFile(Path file, Consumer<Writer> generator) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            generator.accept(writer);
        }
    }
}
