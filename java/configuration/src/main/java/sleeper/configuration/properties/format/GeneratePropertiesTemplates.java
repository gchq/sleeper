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
package sleeper.configuration.properties.format;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.InstancePropertyGroup;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.configuration.properties.table.TablePropertyGroup;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class GeneratePropertiesTemplates {

    private static final Map<InstanceProperty, String> BASIC_INSTANCE_EXAMPLE_VALUES = Map.of(
            ID, "basic-example",
            JARS_BUCKET, "the name of the bucket containing your jars, e.g. sleeper-<insert-unique-name-here>-jars",
            ACCOUNT, "1234567890",
            REGION, "eu-west-2",
            VPC_ID, "1234567890",
            SUBNET, "subnet-abcdefgh");

    private static final Map<TableProperty, String> BASIC_TABLE_EXAMPLE_VALUES = Map.of(
            TABLE_NAME, "example-table",
            ITERATOR_CLASS_NAME, "sleeper.core.iterator.impl.AgeOffIterator",
            ITERATOR_CONFIG, "b,3600000",
            SPLIT_POINTS_FILE, "example/full/splits.txt");

    private GeneratePropertiesTemplates() {
    }

    public static void main(String[] args) throws IOException {
        fromRepositoryPath(Path.of(args[0]));
    }

    public static void fromRepositoryPath(Path repositoryRoot) throws IOException {

        Path fullExampleDir = Files.createDirectories(repositoryRoot.resolve("example/full"));
        writeExampleFullInstanceProperties(
                fullExampleDir.resolve("instance.properties"));
        writeExampleFullTableProperties(
                fullExampleDir.resolve("table.properties"));

        Path basicExampleDir = Files.createDirectories(repositoryRoot.resolve("example/basic"));
        writeExampleBasicInstanceProperties(
                basicExampleDir.resolve("instance.properties"));
        writeExampleBasicTableProperties(
                basicExampleDir.resolve("table.properties"));

        Path scriptsTemplateDir = Files.createDirectories(repositoryRoot.resolve("scripts/templates"));
        writeInstancePropertiesTemplate(
                scriptsTemplateDir.resolve("instanceproperties.template"));
        writeTablePropertiesTemplate(
                scriptsTemplateDir.resolve("tableproperties.template"));
    }

    private static void writeExampleFullInstanceProperties(Path exampleFile) throws IOException {
        InstanceProperties properties = new InstanceProperties();
        BASIC_INSTANCE_EXAMPLE_VALUES.forEach(properties::set);
        properties.set(ID, "full-example");

        // Non-mandatory properties
        properties.set(ECR_INGEST_REPO, "<insert-unique-sleeper-id>/ingest");
        properties.set(BULK_IMPORT_EMR_EC2_KEYPAIR_NAME, "my-key");
        properties.set(BULK_IMPORT_REPO, "<insert-unique-sleeper-id>/bulk-import-runner");
        properties.set(ECR_COMPACTION_REPO, "<insert-unique-sleeper-id>/compaction-job-execution");
        properties.set(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION, "100000");

        writeFullPropertiesTemplate(exampleFile, properties, InstancePropertyGroup.getAll());
    }

    private static void writeExampleFullTableProperties(Path exampleFile) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties properties = new TableProperties(instanceProperties);
        BASIC_TABLE_EXAMPLE_VALUES.forEach(properties::set);
        writeFullPropertiesTemplate(exampleFile, properties, TablePropertyGroup.getAll());
    }

    private static void writeExampleBasicInstanceProperties(Path exampleFile) throws IOException {
        writeBasicPropertiesTemplate(exampleFile,
                new InstanceProperties(),
                InstancePropertyGroup.getAll(),
                BASIC_INSTANCE_EXAMPLE_VALUES);
    }

    private static void writeExampleBasicTableProperties(Path exampleFile) throws IOException {
        writeBasicPropertiesTemplate(exampleFile,
                new TableProperties(new InstanceProperties()),
                TablePropertyGroup.getAll(),
                BASIC_TABLE_EXAMPLE_VALUES);
    }

    private static void writeInstancePropertiesTemplate(Path templateFile) throws IOException {
        InstanceProperties properties = new InstanceProperties();
        BASIC_INSTANCE_EXAMPLE_VALUES.keySet().forEach(property -> properties.set(property, "changeme"));

        Map<Boolean, List<InstanceProperty>> propertiesByIsSet = properties.getPropertiesIndex()
                .getUserDefined().stream().filter(SleeperProperty::isIncludedInTemplate)
                .collect(Collectors.groupingBy(BASIC_INSTANCE_EXAMPLE_VALUES::containsKey));
        List<InstanceProperty> templateProperties = propertiesByIsSet.get(true);
        List<InstanceProperty> defaultProperties = propertiesByIsSet.get(false);

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(templateFile)) {
            PrintWriter writer = new PrintWriter(bufferedWriter);
            writer.println("#################################################################################");
            writer.println("#                           SLEEPER INSTANCE PROPERTIES                         #");
            writer.println("#################################################################################");
            writer.println();
            writer.println("###################");
            writer.println("# Template Values #");
            writer.println("###################");
            SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                            templateProperties, InstancePropertyGroup.getAll(), writer)
                    .print(properties);
            writer.println();
            writer.println();
            writer.println("##################");
            writer.println("# Default Values #");
            writer.println("##################");
            SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                            defaultProperties, InstancePropertyGroup.getAll(), writer)
                    .print(properties);
        }
    }

    private static void writeTablePropertiesTemplate(Path templateFile) throws IOException {
        TableProperties properties = new TableProperties(new InstanceProperties());
        properties.set(TABLE_NAME, "changeme");

        List<TableProperty> templateProperties = List.of(
                TABLE_NAME, ROW_GROUP_SIZE, PAGE_SIZE, COMPRESSION_CODEC,
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, STATESTORE_CLASSNAME);

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(templateFile)) {
            PrintWriter writer = new PrintWriter(bufferedWriter);
            writer.println("#################################################################################");
            writer.println("#                           SLEEPER TABLE PROPERTIES                            #");
            writer.println("#################################################################################");
            writer.println();
            writer.println("###################");
            writer.println("# Template Values #");
            writer.println("###################");
            SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                            templateProperties, TablePropertyGroup.getAll(), writer)
                    .print(properties);
        }
    }

    private static <T extends SleeperProperty> void writeFullPropertiesTemplate(
            Path file, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups) throws IOException {
        writePropertiesTemplate(file, properties, propertyGroups,
                properties.getPropertiesIndex().getUserDefined().stream());
    }

    private static <T extends SleeperProperty> void writeBasicPropertiesTemplate(
            Path file, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups, Map<T, String> basicValues) throws IOException {
        basicValues.forEach(properties::set);
        writePropertiesTemplate(file, properties, propertyGroups,
                properties.getPropertiesIndex().getUserDefined().stream()
                        .filter(property -> property.isIncludedInBasicTemplate()
                                || basicValues.containsKey(property)));
    }

    private static <T extends SleeperProperty> void writePropertiesTemplate(
            Path file,
            SleeperProperties<T> properties,
            List<PropertyGroup> propertyGroups,
            Stream<T> propertyDefinitions) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                            propertyDefinitions.filter(SleeperProperty::isIncludedInTemplate)
                                    .collect(Collectors.toList()),
                            propertyGroups, new PrintWriter(writer))
                    .print(properties);
        }
    }
}
