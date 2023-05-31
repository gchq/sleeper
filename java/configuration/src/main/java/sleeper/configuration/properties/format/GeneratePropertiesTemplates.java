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
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class GeneratePropertiesTemplates {

    private static final Map<InstanceProperty, String> BASIC_INSTANCE_TEMPLATE_VALUES = Map.of(
            ID, "full-example",
            JARS_BUCKET, "the name of the bucket containing your jars, e.g. sleeper-<insert-unique-name-here>-jars",
            ACCOUNT, "1234567890",
            REGION, "eu-west-2",
            VPC_ID, "1234567890",
            SUBNET, "subnet-abcdefgh");

    private static final Map<TableProperty, String> BASIC_TABLE_TEMPLATE_VALUES = Map.of(
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
    }

    private static void writeExampleFullInstanceProperties(Path exampleFile) throws IOException {
        InstanceProperties properties = new InstanceProperties();
        BASIC_INSTANCE_TEMPLATE_VALUES.forEach(properties::set);

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
        BASIC_TABLE_TEMPLATE_VALUES.forEach(properties::set);
        writeFullPropertiesTemplate(exampleFile, properties, TablePropertyGroup.getAll());
    }

    private static void writeExampleBasicInstanceProperties(Path exampleFile) throws IOException {
        writeBasicPropertiesTemplate(exampleFile,
                new InstanceProperties(),
                InstancePropertyGroup.getAll(),
                BASIC_INSTANCE_TEMPLATE_VALUES);
    }

    private static void writeExampleBasicTableProperties(Path exampleFile) throws IOException {
        writeBasicPropertiesTemplate(exampleFile,
                new TableProperties(new InstanceProperties()),
                TablePropertyGroup.getAll(),
                BASIC_TABLE_TEMPLATE_VALUES);
    }

    private static <T extends SleeperProperty> void writeFullPropertiesTemplate(
            Path file, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups) throws IOException {
        writePropertiesTemplate(file, properties,
                properties.getPropertiesIndex().getUserDefined().stream(),
                propertyGroups);
    }

    private static <T extends SleeperProperty> void writeBasicPropertiesTemplate(
            Path file, SleeperProperties<T> properties, List<PropertyGroup> propertyGroups, Map<T, String> basicValues) throws IOException {
        basicValues.forEach(properties::set);
        writePropertiesTemplate(file, properties,
                properties.getPropertiesIndex().getUserDefined().stream()
                        .filter(property -> property.isIncludedInBasicTemplate()
                                || basicValues.containsKey(property)),
                propertyGroups);
    }

    private static <T extends SleeperProperty> void writePropertiesTemplate(
            Path file, SleeperProperties<T> properties,
            Stream<T> propertyDefinitions, List<PropertyGroup> propertyGroups) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            SleeperPropertiesPrettyPrinter.forPropertiesTemplate(
                            propertyDefinitions.filter(SleeperProperty::isIncludedInTemplate)
                                    .collect(Collectors.toList()),
                            propertyGroups, new PrintWriter(writer))
                    .print(properties);
        }
    }
}
