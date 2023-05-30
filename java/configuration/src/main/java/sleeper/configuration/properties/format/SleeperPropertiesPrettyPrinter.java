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

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang.WordUtils;

import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.InstancePropertyGroup;
import sleeper.configuration.properties.PropertiesUtils;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.configuration.properties.table.TablePropertyGroup;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SleeperPropertiesPrettyPrinter<T extends SleeperProperty> {

    private final List<T> sortedProperties;
    private final PrintWriter writer;
    private final PropertiesConfiguration.PropertiesWriter propertiesWriter;
    private final boolean hideUnknownProperties;
    private final boolean commentUnsetProperties;

    SleeperPropertiesPrettyPrinter(List<T> properties, List<PropertyGroup> groups, PrintWriter writer) {
        this(properties, groups, writer, false, true);
    }

    private SleeperPropertiesPrettyPrinter(
            List<T> properties, List<PropertyGroup> groups, PrintWriter writer,
            boolean hideUnknownProperties, boolean commentUnsetProperties) {
        this.sortedProperties = PropertyGroup.sortPropertiesByGroup(properties, groups);
        this.writer = writer;
        this.propertiesWriter = PropertiesUtils.buildPropertiesWriter(writer);
        this.hideUnknownProperties = hideUnknownProperties;
        this.commentUnsetProperties = commentUnsetProperties;
    }

    public static <T extends SleeperProperty> SleeperPropertiesPrettyPrinter<T> setAllProperties(
            List<T> properties, List<PropertyGroup> groups, PrintWriter writer) {
        return new SleeperPropertiesPrettyPrinter<>(
                properties, groups, writer, false, false);
    }

    public static SleeperPropertiesPrettyPrinter<InstanceProperty> forInstanceProperties(PrintWriter writer) {
        return new SleeperPropertiesPrettyPrinter<>(InstanceProperty.getAll(), InstancePropertyGroup.getAll(), writer);
    }

    public static SleeperPropertiesPrettyPrinter<InstanceProperty> forInstancePropertiesWithGroup(
            PrintWriter writer, PropertyGroup group) {
        return new SleeperPropertiesPrettyPrinter<>(InstanceProperty.getAll().stream()
                .filter(property -> property.getPropertyGroup().equals(group))
                .collect(Collectors.toList()), List.of(group), writer, true, true);
    }

    public static SleeperPropertiesPrettyPrinter<TableProperty> forTableProperties(PrintWriter writer) {
        return new SleeperPropertiesPrettyPrinter<>(TableProperty.getAll(), TablePropertyGroup.getAll(), writer);
    }

    public static SleeperPropertiesPrettyPrinter<TableProperty> forTablePropertiesWithGroup(
            PrintWriter writer, PropertyGroup group) {
        return new SleeperPropertiesPrettyPrinter<>(TableProperty.getAll().stream()
                .filter(property -> property.getPropertyGroup().equals(group))
                .collect(Collectors.toList()), List.of(group), writer, true, true);
    }

    public void print(SleeperProperties<T> properties) {
        PropertyGroup currentGroup = null;
        for (T property : sortedProperties) {
            if (currentGroup == null || !currentGroup.equals(property.getPropertyGroup())) {
                currentGroup = property.getPropertyGroup();
                println();
                println(formatDescription(currentGroup));
            }
            println();
            println(formatDescription(property));
            if (property.isSystemDefined()) {
                println("# (this property is system-defined and may not be edited)");
            }
            String value = properties.get(property);
            if (value != null) {
                if (!properties.isSet(property)) {
                    println("# (using default value shown below, uncomment to set a value)");
                    print("# ");
                }
                printProperty(property.getPropertyName(), value);
            } else {
                if (commentUnsetProperties) {
                    println("# (no value set, uncomment to set a value)");
                    print("# ");
                }
                printProperty(property.getPropertyName(), "");
            }
        }
        if (!hideUnknownProperties) {
            Map<String, String> unknownProperties = properties.getUnknownProperties()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!unknownProperties.isEmpty()) {
                println();
                println("# The following properties are not recognised by Sleeper.");
                unknownProperties.keySet().stream().sorted().forEach(name ->
                        printProperty(name, unknownProperties.get(name)));
            }
        }
        writer.flush();
    }

    private void println(String line) {
        writer.println(line);
    }

    private void println() {
        writer.println();
    }

    private void print(String value) {
        writer.print(value);
    }

    private void printProperty(String name, String value) {
        try {
            propertiesWriter.writeProperty(name, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String formatDescription(SleeperProperty property) {
        return formatDescription(property.getDescription());
    }

    private static String formatDescription(PropertyGroup group) {
        return formatDescription(group.getDescription());
    }

    private static String formatDescription(String description) {
        return formatDescription("# ", description);
    }

    public static String formatDescription(String lineStart, String description) {
        return Arrays.stream(description.split("\n")).
                map(line -> lineStart + WordUtils.wrap(line, 100).replace("\n", "\n" + lineStart))
                .collect(Collectors.joining("\n"));
    }
}
