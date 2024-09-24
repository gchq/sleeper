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
package sleeper.clients.util;

import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.format.GeneratePropertiesTemplates;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.function.Consumer;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

public class ComparePropertiesTemplates {

    private ComparePropertiesTemplates() {
    }

    public static void main(String[] args) throws IOException {
        fromRepositoryPath(Path.of(args[0]), new ConsoleOutput(System.out));
    }

    public static void fromRepositoryPath(Path repositoryRoot, ConsoleOutput out) throws IOException {
        Path fullExampleDir = Files.createDirectories(repositoryRoot.resolve("example/full"));
        Path basicExampleDir = Files.createDirectories(repositoryRoot.resolve("example/basic"));
        Path scriptsTemplateDir = Files.createDirectories(repositoryRoot.resolve("scripts/templates"));

        InstanceProperties fullInstanceProperties = loadInstanceProperties(fullExampleDir.resolve("instance.properties"));
        InstanceProperties basicInstanceProperties = loadInstanceProperties(basicExampleDir.resolve("instance.properties"));
        InstanceProperties templateInstanceProperties = loadInstanceProperties(scriptsTemplateDir.resolve("instanceproperties.template"));

        printDiff(out, "Full instance properties example",
                fullInstanceProperties,
                generateInstanceProperties(GeneratePropertiesTemplates::writeExampleFullInstanceProperties));
        printDiff(out, "Full table properties example",
                loadTableProperties(fullInstanceProperties, fullExampleDir.resolve("table.properties")),
                generateTableProperties(fullInstanceProperties, GeneratePropertiesTemplates::writeExampleFullTableProperties));
        printDiff(out, "Basic instance properties example",
                basicInstanceProperties,
                generateInstanceProperties(GeneratePropertiesTemplates::writeExampleBasicInstanceProperties));
        printDiff(out, "Basic table properties example",
                loadTableProperties(basicInstanceProperties, basicExampleDir.resolve("table.properties")),
                generateTableProperties(basicInstanceProperties, GeneratePropertiesTemplates::writeExampleBasicTableProperties));
        printDiff(out, "Instance properties template",
                templateInstanceProperties,
                generateInstanceProperties(GeneratePropertiesTemplates::writeInstancePropertiesTemplate));
        printDiff(out, "Table properties template",
                loadTableProperties(templateInstanceProperties, scriptsTemplateDir.resolve("tableproperties.template")),
                generateTableProperties(templateInstanceProperties, GeneratePropertiesTemplates::writeTablePropertiesTemplate));
    }

    private static <T extends SleeperProperty> void printDiff(
            ConsoleOutput out, String description, SleeperProperties<T> before, SleeperProperties<T> after) {
        out.println(description);
        out.println("=".repeat(description.length()));
        PropertiesDiff diff = new PropertiesDiff(before, after);
        if (!diff.isChanged()) {
            out.println("Found no changes");
            out.println();
            out.println();
            return;
        }
        diff.print(out, before.getPropertiesIndex(), Collections.emptySet());
        out.println();
    }

    private static InstanceProperties loadInstanceProperties(Path file) {
        return InstanceProperties.createWithoutValidation(loadProperties(file));
    }

    private static TableProperties loadTableProperties(InstanceProperties instanceProperties, Path file) {
        TableProperties properties = new TableProperties(instanceProperties, loadProperties(file));
        properties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .build());
        properties.validate();
        return properties;
    }

    private static InstanceProperties generateInstanceProperties(Consumer<Writer> generator) {
        StringWriter writer = new StringWriter();
        generator.accept(writer);
        return InstanceProperties.createWithoutValidation(loadProperties(writer.toString()));
    }

    private static TableProperties generateTableProperties(
            InstanceProperties instanceProperties, Consumer<Writer> generator) {
        StringWriter writer = new StringWriter();
        generator.accept(writer);
        TableProperties properties = new TableProperties(instanceProperties, loadProperties(writer.toString()));
        properties.setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .build());
        properties.validate();
        return properties;
    }
}
