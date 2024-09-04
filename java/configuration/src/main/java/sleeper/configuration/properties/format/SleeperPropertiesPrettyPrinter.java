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
package sleeper.configuration.properties.format;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang.WordUtils;

import sleeper.configuration.properties.PropertiesUtils;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.instance.InstancePropertyGroup;
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
    private final boolean printTemplate;

    private SleeperPropertiesPrettyPrinter(Builder<T> builder) {
        sortedProperties = builder.sortedProperties;
        writer = builder.writer;
        propertiesWriter = PropertiesUtils.buildPropertiesWriter(writer);
        hideUnknownProperties = builder.hideUnknownProperties;
        printTemplate = builder.printTemplate;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    public static <T extends SleeperProperty> SleeperPropertiesPrettyPrinter<T> forPropertiesTemplate(
            List<T> properties, List<PropertyGroup> groups, PrintWriter writer) {
        return builder().properties(properties, groups)
                .writer(writer).printTemplate(true).build();
    }

    public static SleeperPropertiesPrettyPrinter<InstanceProperty> forInstanceProperties(PrintWriter writer) {
        return builder().properties(InstanceProperty.getAll(), InstancePropertyGroup.getAll())
                .writer(writer).build();
    }

    public static SleeperPropertiesPrettyPrinter<InstanceProperty> forInstancePropertiesWithGroup(
            PrintWriter writer, PropertyGroup group) {
        return builder().sortedProperties(InstanceProperty.getAll().stream()
                .filter(property -> property.getPropertyGroup().equals(group))
                .collect(Collectors.toList()))
                .writer(writer).hideUnknownProperties(true).build();
    }

    public static SleeperPropertiesPrettyPrinter<TableProperty> forTableProperties(PrintWriter writer) {
        return builder().properties(TableProperty.getAll(), TablePropertyGroup.getAll())
                .writer(writer).build();
    }

    public static SleeperPropertiesPrettyPrinter<TableProperty> forTablePropertiesWithGroup(
            PrintWriter writer, PropertyGroup group) {
        return builder().sortedProperties(TableProperty.getAll().stream()
                .filter(property -> property.getPropertyGroup().equals(group))
                .collect(Collectors.toList()))
                .writer(writer).hideUnknownProperties(true).build();
    }

    public void print(SleeperProperties<T> properties) {
        PropertyGroup currentGroup = null;
        for (T property : sortedProperties) {
            if (currentGroup == null) {
                currentGroup = property.getPropertyGroup();
                println();
                println(formatDescription(currentGroup));
            } else if (!currentGroup.equals(property.getPropertyGroup())) {
                currentGroup = property.getPropertyGroup();
                println();
                println();
                println(formatDescription(currentGroup));
            }
            printProperty(properties, property);
        }
        if (!hideUnknownProperties) {
            Map<String, String> unknownProperties = properties.getUnknownProperties()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!unknownProperties.isEmpty()) {
                println();
                println("# The following properties are not recognised by Sleeper.");
                unknownProperties.keySet().stream().sorted().forEach(name -> printSetPropertyValue(name, unknownProperties.get(name)));
            }
        }
        writer.flush();
    }

    private void printProperty(SleeperProperties<T> properties, T property) {
        println();
        println(formatDescription(property));
        if (!property.isUserDefined()) {
            println("# (this property is system-defined and may not be edited)");
        }
        String value = properties.get(property);
        if (value != null) {
            if (!properties.isSet(property) && !printTemplate) {
                println("# (using default value shown below, uncomment to set a value)");
                print("# ");
            }
            printSetPropertyValue(property.getPropertyName(), value);
        } else {
            if (!printTemplate) {
                println("# (no value set, uncomment to set a value)");
            }
            print("# ");
            printSetPropertyValue(property.getPropertyName(), "");
        }
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

    private void printSetPropertyValue(String name, String value) {
        try {
            propertiesWriter.writeProperty(name, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String formatDescription(SleeperProperty property) {
        return formatDescription("# ", property.getDescription());
    }

    private static String formatDescription(PropertyGroup group) {
        return formatDescription("## ", group.getDescription());
    }

    public static String formatDescription(String lineStart, String description) {
        return Arrays.stream(description.split("\n")).map(line -> lineStart + WordUtils.wrap(line, 100).replace("\n", "\n" + lineStart))
                .collect(Collectors.joining("\n"));
    }

    public static final class Builder<T extends SleeperProperty> {
        private List<T> sortedProperties;
        private PrintWriter writer;
        private boolean hideUnknownProperties;
        private boolean printTemplate;

        private Builder() {
        }

        @SuppressWarnings("unchecked")
        public <P extends SleeperProperty> Builder<P> sortedProperties(List<P> sortedProperties) {
            this.sortedProperties = (List<T>) sortedProperties;
            return (Builder<P>) this;
        }

        public <P extends SleeperProperty> Builder<P> properties(List<P> properties, List<PropertyGroup> groups) {
            return sortedProperties(PropertyGroup.sortPropertiesByGroup(properties, groups));
        }

        public Builder<T> writer(PrintWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder<T> hideUnknownProperties(boolean hideUnknownProperties) {
            this.hideUnknownProperties = hideUnknownProperties;
            return this;
        }

        public Builder<T> printTemplate(boolean printTemplate) {
            this.printTemplate = printTemplate;
            return this;
        }

        public SleeperPropertiesPrettyPrinter<T> build() {
            return new SleeperPropertiesPrettyPrinter<>(this);
        }
    }
}
