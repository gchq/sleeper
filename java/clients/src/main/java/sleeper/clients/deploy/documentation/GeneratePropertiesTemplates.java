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
import sleeper.core.properties.table.TablePropertyGroup;
import sleeper.systemtest.configuration.SystemTestProperties;

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
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_DRIVER_CORES;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_EPHEMERAL_STORAGE;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.core.properties.model.OptionalStack.DEFAULT_STACKS;

/**
 * Generates template files to be filled in when deploying an instance of Sleeper, or creating tables.
 */
public class GeneratePropertiesTemplates {

    private static final SystemTestProperties DEMO_INSTANCE_PROPERTIES = DemoDeploymentTemplate.createInstanceProperties();
    private static final TableProperties DEMO_TABLE_PROPERTIES = DemoDeploymentTemplate.createTableProperties(DEMO_INSTANCE_PROPERTIES);

    private GeneratePropertiesTemplates() {
    }

    public static void main(String[] args) throws Exception {
        createTemplates(args.length < 1 ? Path.of(".") : Path.of(args[0]));
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

        Path lightTemplateDir = Files.createDirectories(repositoryRoot.resolve("example/light"));
        writeFile(lightTemplateDir.resolve("instance.properties"),
                GeneratePropertiesTemplates::writeExampleLightInstanceProperties);
        writeFile(lightTemplateDir.resolve("table.properties"),
                GeneratePropertiesTemplates::writeExampleBasicTableProperties);

        Path demoDeploymentDir = Files.createDirectories(repositoryRoot.resolve("scripts/test/deployAll"));
        writeFile(demoDeploymentDir.resolve("system-test-instance.properties.template"),
                GeneratePropertiesTemplates::writeInstancePropertiesDemoTemplate);
        writeFile(demoDeploymentDir.resolve("table.properties.template"),
                GeneratePropertiesTemplates::writeTablePropertiesDemoTemplate);
    }

    private static void writeExampleFullInstanceProperties(Writer writer) {
        InstanceProperties properties = new InstanceProperties();

        writeFullPropertiesTemplate(writer, properties, InstancePropertyGroup.getAll());
    }

    private static void writeExampleFullTableProperties(Writer writer) {
        TableProperties properties = new TableProperties(new InstanceProperties());

        writeFullPropertiesTemplate(writer, properties, TablePropertyGroup.getAll());
    }

    private static void writeExampleBasicInstanceProperties(Writer writer) {
        writeBasicPropertiesTemplate(writer,
                new InstanceProperties(),
                InstancePropertyGroup.getAll());
    }

    private static void writeExampleBasicTableProperties(Writer writer) {
        writeBasicPropertiesTemplate(writer,
                new TableProperties(new InstanceProperties()),
                TablePropertyGroup.getAll());
    }

    private static void writeExampleLightInstanceProperties(Writer out) {
        InstanceProperties instanceProperties = new InstanceProperties();
        // Emr Serverless properties
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY, "8G");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK, "60G");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES, "2");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY, "8G");

        // EKS properties
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY, "8G");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_EPHEMERAL_STORAGE, "60Gi");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_INSTANCES, "2");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_DRIVER_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY, "8G");

        // Default table values
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "8");
        instanceProperties.set(DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS, "1200");

        // Stack
        instanceProperties.set(OPTIONAL_STACKS, DEFAULT_STACKS.stream().map(stack -> stack.name()).collect(Collectors.joining(",")));

        List<InstanceProperty> propertiesByIsSet = instanceProperties.streamNonDefaultEntries().map(entry -> entry.getKey()).toList();

        PrintWriter writer = new PrintWriter(out);
        writer.println("""
                #################################################################################
                #                    Properties set below are designed for an                   #
                #                  instance aimed towards reducing running costs                #
                #               and will apply to any bulk import stacks you enable             #
                #################################################################################""");
        writer.println();
        SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                propertiesByIsSet, InstancePropertyGroup.getAll(), writer)
                .print(instanceProperties);
    }

    private static void writeInstancePropertiesDemoTemplate(Writer out) {
        PrintWriter writer = new PrintWriter(out);
        writer.println("""
                ########################################################################################
                #                              System Test Properties                                  #
                ########################################################################################

                # Test runs will use a copy of this file with the same name but without `.template` on the end.
                # Please do not edit the template. If you do not create the copy it will be created automatically.""");
        writer.println();
        SystemTestProperties.createSystemTestPrettyPrinterBuilder()
                .writer(writer)
                .hideUnsetProperties(true)
                .printTemplate(true)
                .build().print(DEMO_INSTANCE_PROPERTIES);
    }

    private static void writeTablePropertiesDemoTemplate(Writer out) {
        PrintWriter writer = new PrintWriter(out);
        TableProperties.createPrettyPrinterBuilder()
                .writer(writer)
                .hideUnsetProperties(true)
                .printTemplate(true)
                .build().print(DEMO_TABLE_PROPERTIES);
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
