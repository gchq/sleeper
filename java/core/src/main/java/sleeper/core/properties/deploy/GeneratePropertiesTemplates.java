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
package sleeper.core.properties.deploy;

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
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_GPU_REPO;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.core.properties.instance.IngestProperty.ECR_INGEST_REPO;
import static sleeper.core.properties.instance.LoggingLevelsProperty.APACHE_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.AWS_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.PARQUET_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.ROOT_LOGGING_LEVEL;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Generates template files to be filled in when deploying an instance of Sleeper, or creating tables.
 */
public class GeneratePropertiesTemplates {

    private static final Map<InstanceProperty, String> BASIC_INSTANCE_EXAMPLE_VALUES = Map.of(
            ID, "basic-example",
            JARS_BUCKET, "the name of the bucket containing your jars, e.g. sleeper-<insert-unique-name-here>-jars",
            ACCOUNT, "1234567890",
            REGION, "eu-west-2",
            VPC_ID, "1234567890",
            SUBNETS, "subnet-abcdefgh");

    private static final Map<InstanceProperty, String> EMR_REPOSITORY_EXAMPLE_VALUES = Map.of(
            ECR_INGEST_REPO, "<insert-unique-sleeper-id>/ingest",
            BULK_IMPORT_REPO, "<insert-unique-sleeper-id>/bulk-import-runner",
            BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, "<insert-unique-sleeper-id>/bulk-import-runner-emr-serverless",
            ECR_COMPACTION_GPU_REPO, "<insert-unique-sleeper-id>/compaction-gpu",
            ECR_COMPACTION_REPO, "<insert-unique-sleeper-id>/compaction-job-execution");

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

    /**
     * Generates and writes all template files.
     *
     * @param  repositoryRoot the root directory of the Sleeper repository
     * @throws IOException    if any files could not be written
     */
    public static void fromRepositoryPath(Path repositoryRoot) throws IOException {

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

    private static InstanceProperties generateExampleFullInstanceProperties() {
        InstanceProperties properties = new InstanceProperties();
        BASIC_INSTANCE_EXAMPLE_VALUES.forEach(properties::set);
        properties.set(ID, "full-example");

        // Non-mandatory properties
        EMR_REPOSITORY_EXAMPLE_VALUES.forEach(properties::set);
        properties.set(BULK_IMPORT_EMR_EC2_KEYPAIR_NAME, "my-key");
        properties.set(DEFAULT_SIZERATIO_COMPACTION_STRATEGY_MAX_CONCURRENT_JOBS_PER_PARTITION, "100000");
        properties.set(LOGGING_LEVEL, "INFO");
        properties.set(ROOT_LOGGING_LEVEL, "INFO");
        properties.set(APACHE_LOGGING_LEVEL, "INFO");
        properties.set(PARQUET_LOGGING_LEVEL, "WARN");
        properties.set(AWS_LOGGING_LEVEL, "INFO");

        return properties;
    }

    private static InstanceProperties generateTemplateInstanceProperties() {
        InstanceProperties properties = new InstanceProperties();
        BASIC_INSTANCE_EXAMPLE_VALUES.keySet().forEach(property -> properties.set(property, "changeme"));
        EMR_REPOSITORY_EXAMPLE_VALUES.keySet().forEach(property -> properties.set(property, "changeme"));
        return properties;
    }

    private static TableProperties generateExampleTableProperties() {
        TableProperties properties = new TableProperties(new InstanceProperties());
        BASIC_TABLE_EXAMPLE_VALUES.forEach(properties::set);
        return properties;
    }

    private static TableProperties generateTemplateTableProperties() {
        TableProperties properties = new TableProperties(new InstanceProperties());
        properties.set(TABLE_NAME, "changeme");
        return properties;
    }

    /**
     * Writes the full instance properties example file to the given writer.
     *
     * @param writer the writer
     */
    public static void writeExampleFullInstanceProperties(Writer writer) {
        InstanceProperties properties = generateExampleFullInstanceProperties();
        writeFullPropertiesTemplate(writer, properties, InstancePropertyGroup.getAll());
    }

    /**
     * Writes the full table properties example file to the given writer.
     *
     * @param writer the writer
     */
    public static void writeExampleFullTableProperties(Writer writer) {
        TableProperties properties = generateExampleTableProperties();
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
                InstancePropertyGroup.getAll(),
                BASIC_INSTANCE_EXAMPLE_VALUES);
    }

    /**
     * Writes the basic table properties example file to the given writer.
     *
     * @param writer the writer
     */
    public static void writeExampleBasicTableProperties(Writer writer) {
        writeBasicPropertiesTemplate(writer,
                new TableProperties(new InstanceProperties()),
                TablePropertyGroup.getAll(),
                BASIC_TABLE_EXAMPLE_VALUES);
    }

    /**
     * Writes the instance properties template file to the given writer.
     *
     * @param out the writer
     */
    public static void writeInstancePropertiesTemplate(Writer out) {
        InstanceProperties properties = generateTemplateInstanceProperties();

        Map<Boolean, List<InstanceProperty>> propertiesByIsSet = properties.getPropertiesIndex()
                .getUserDefined().stream().filter(SleeperProperty::isIncludedInTemplate)
                .collect(Collectors.groupingBy(properties::isSet));
        List<InstanceProperty> templateProperties = propertiesByIsSet.get(true);
        List<InstanceProperty> defaultProperties = propertiesByIsSet.get(false);

        PrintWriter writer = new PrintWriter(out);
        writer.println("#################################################################################");
        writer.println("#                           SLEEPER INSTANCE PROPERTIES                         #");
        writer.println("#################################################################################");
        writer.println();
        writer.println("###################");
        writer.println("# Template Values #");
        writer.println("###################");
        SleeperPropertiesPrettyPrinter.builderForPropertiesTemplate(
                templateProperties, InstancePropertyGroup.getAll(), writer)
                .printGroupDetails(false)
                .build().print(properties);
        writer.println();
        writer.println();
        writer.println("##################");
        writer.println("# Default Values #");
        writer.println("##################");
        SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                defaultProperties, InstancePropertyGroup.getAll(), writer)
                .print(properties);
    }

    /**
     * Writes the table properties template file to the given writer.
     *
     * @param out the writer
     */
    public static void writeTablePropertiesTemplate(Writer out) {
        TableProperties properties = generateTemplateTableProperties();

        List<TableProperty> templateProperties = List.of(
                TABLE_NAME, ROW_GROUP_SIZE, PAGE_SIZE, COMPRESSION_CODEC,
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, STATESTORE_CLASSNAME);

        PrintWriter writer = new PrintWriter(out);
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

    private static <T extends SleeperProperty> void writeFullPropertiesTemplate(
            Writer writer, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups) {
        writePropertiesTemplate(writer, properties, propertyGroups,
                properties.getPropertiesIndex().getUserDefined().stream());
    }

    private static <T extends SleeperProperty> void writeBasicPropertiesTemplate(
            Writer writer, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups, Map<T, String> basicValues) {
        basicValues.forEach(properties::set);
        writePropertiesTemplate(writer, properties, propertyGroups,
                properties.getPropertiesIndex().getUserDefined().stream()
                        .filter(property -> property.isIncludedInBasicTemplate()
                                || basicValues.containsKey(property)));
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
