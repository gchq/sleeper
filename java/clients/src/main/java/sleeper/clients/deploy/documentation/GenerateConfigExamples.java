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
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperties;
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

import static java.util.function.Predicate.not;

/**
 * Generates example configurations to deploy a Sleeper instance and/or tables.
 */
public class GenerateConfigExamples {

    private GenerateConfigExamples() {
    }

    public static void main(String[] args) throws Exception {
        writeFiles(args.length < 1 ? Path.of(".") : Path.of(args[0]));
    }

    /**
     * Generates and writes all example files.
     *
     * @param  repositoryRoot the root directory of the Sleeper repository
     * @throws IOException    if any files could not be written
     */
    public static void writeFiles(Path repositoryRoot) throws IOException {
        writeFullExample(Files.createDirectories(repositoryRoot.resolve("example/full")));
        writeBasicExample(Files.createDirectories(repositoryRoot.resolve("example/basic")));
        writeLightExample(Files.createDirectories(repositoryRoot.resolve("example/light")));
        writeDemoDeploymentTemplates(Files.createDirectories(repositoryRoot.resolve("scripts/test/deployAll")));
    }

    private static void writeFullExample(Path fullExampleDir) throws IOException {
        writeFile(fullExampleDir.resolve("instance.properties"),
                writer -> writeFullPropertiesTemplate(writer,
                        new InstanceProperties(),
                        InstancePropertyGroup.getAll()));
        writeFile(fullExampleDir.resolve("table.properties"),
                writer -> writeFullPropertiesTemplate(writer,
                        new TableProperties(new InstanceProperties()),
                        TablePropertyGroup.getAll()));
    }

    private static void writeBasicExample(Path basicExampleDir) throws IOException {
        writeFile(basicExampleDir.resolve("instance.properties"),
                writer -> writeBasicPropertiesTemplate(writer,
                        new InstanceProperties(),
                        InstancePropertyGroup.getAll()));
        writeFile(basicExampleDir.resolve("table.properties"),
                writer -> writeBasicPropertiesTemplate(writer,
                        new TableProperties(new InstanceProperties()),
                        TablePropertyGroup.getAll()));
    }

    private static void writeLightExample(Path lightExampleDir) throws IOException {
        writeFile(lightExampleDir.resolve("instance.properties"),
                LightExampleConfig::writeExampleLightInstanceProperties);
        writeFile(lightExampleDir.resolve("table.properties"),
                writer -> writeBasicPropertiesTemplate(writer,
                        new TableProperties(new InstanceProperties()),
                        TablePropertyGroup.getAll()));
    }

    private static void writeDemoDeploymentTemplates(Path demoDeploymentDir) throws IOException {
        writeFile(demoDeploymentDir.resolve("system-test-instance.properties.template"),
                DemoDeploymentTemplates::writeInstancePropertiesTemplate);
        writeFile(demoDeploymentDir.resolve("tags.properties.template"),
                DemoDeploymentTemplates::writeTagsTemplate);
        writeFile(demoDeploymentDir.resolve("table.properties.template"),
                DemoDeploymentTemplates::writeTablePropertiesTemplate);
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
        SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                properties.getPropertiesIndex().getUserDefined().stream()
                        .filter(SleeperProperty::isIncludedInBasicTemplate)
                        .filter(SleeperProperty::isIncludedInTemplate)
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
