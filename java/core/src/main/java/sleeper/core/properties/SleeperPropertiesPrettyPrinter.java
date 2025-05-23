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
package sleeper.core.properties;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.text.WordUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A printer to output the property values in a human-readable string format.
 *
 * @param <T> the type of properties that may be printed
 */
public class SleeperPropertiesPrettyPrinter<T extends SleeperProperty> {

    private final List<T> sortedProperties;
    private final PrintWriter writer;
    private final PropertiesConfiguration.PropertiesWriter propertiesWriter;
    private final boolean hideUnknownProperties;
    private final boolean printTemplate;
    private final boolean printGroupDetails;

    private SleeperPropertiesPrettyPrinter(Builder<T> builder) {
        sortedProperties = builder.sortedProperties;
        writer = builder.writer;
        propertiesWriter = PropertiesUtils.buildPropertiesWriter(writer);
        hideUnknownProperties = builder.hideUnknownProperties;
        printTemplate = builder.printTemplate;
        printGroupDetails = builder.printGroupDetails;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    /**
     * Creates a printer to be used to generate a properties template.
     *
     * @param  <T>        the type of properties to be printed
     * @param  properties the properties to be printed
     * @param  groups     the groups to organise properties into
     * @param  writer     the writer to write to
     * @return            the pretty printer
     */
    public static <T extends SleeperProperty> SleeperPropertiesPrettyPrinter<T> forPropertiesTemplate(
            List<T> properties, List<PropertyGroup> groups, PrintWriter writer) {
        return builder().properties(properties, groups)
                .writer(writer).printTemplate(true).build();
    }

    /**
     * Creates a builder for a printer to be used to generate a properties template.
     *
     * @param  <T>        the type of properties to be printed
     * @param  properties the properties to be printed
     * @param  groups     the groups to organise properties into
     * @param  writer     the writer to write to
     * @return            the builder
     */
    public static <T extends SleeperProperty> SleeperPropertiesPrettyPrinter.Builder<T> builderForPropertiesTemplate(
            List<T> properties, List<PropertyGroup> groups, PrintWriter writer) {
        return builder().properties(properties, groups)
                .writer(writer).printTemplate(true);
    }

    /**
     * Pretty prints the given property values.
     *
     * @param properties the property values
     */
    public void print(SleeperProperties<T> properties) {
        PropertyGroup currentGroup = null;
        for (T property : sortedProperties) {
            if (currentGroup == null) {
                currentGroup = property.getPropertyGroup();
                printGroupHeader(currentGroup);
            } else if (!currentGroup.equals(property.getPropertyGroup())) {
                currentGroup = property.getPropertyGroup();
                println();
                printGroupHeader(currentGroup);
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

    private void printGroupHeader(PropertyGroup group) {
        println();
        println(formatDescription("## ", group.getDescription()));
        if (printGroupDetails && group.getDetails() != null) {
            println("## ");
            println(formatDescription("## ", group.getDetails()));
        }
    }

    private void printProperty(SleeperProperties<T> properties, T property) {
        println();
        println(formatDescription("# ", property.getDescription()));
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

    /**
     * Formats a property description with line wrapping for a short line length.
     *
     * @param  lineStart   prefix for every line of the description
     * @param  description the description
     * @return             the formatted description
     */
    public static String formatDescription(String lineStart, String description) {
        return Arrays.stream(description.split("\n")).map(line -> lineStart + WordUtils.wrap(line, 100).replace("\n", "\n" + lineStart))
                .collect(Collectors.joining("\n"));
    }

    /**
     * Builder to create an instance of this class.
     *
     * @param <T> the type of properties that may be printed
     */
    public static final class Builder<T extends SleeperProperty> {
        private List<T> sortedProperties;
        private PrintWriter writer;
        private boolean hideUnknownProperties;
        private boolean printTemplate;
        private boolean printGroupDetails = true;

        private Builder() {
        }

        /**
         * Sets which properties will be printed.
         *
         * @param  <P>              the type of properties to be printed
         * @param  sortedProperties the properties to be printed
         * @return                  this builder
         */
        @SuppressWarnings("unchecked")
        public <P extends SleeperProperty> Builder<P> sortedProperties(List<P> sortedProperties) {
            this.sortedProperties = (List<T>) sortedProperties;
            return (Builder<P>) this;
        }

        /**
         * Sets which properties will be printed, and orders them to match the given order of groups.
         *
         * @param  <P>        the type of properties to be printed
         * @param  properties the properties to be printed
         * @param  groups     the order in which to display property groups
         * @return            this builder
         */
        public <P extends SleeperProperty> Builder<P> properties(List<P> properties, List<PropertyGroup> groups) {
            return sortedProperties(PropertyGroup.sortPropertiesByGroup(properties, groups));
        }

        /**
         * Sets the writer to write to.
         *
         * @param  writer the writer
         * @return        this builder
         */
        public Builder<T> writer(PrintWriter writer) {
            this.writer = writer;
            return this;
        }

        /**
         * Sets whether to display properties that have no definition for the expected type.
         *
         * @param  hideUnknownProperties true to hide unknown properties, false to display them
         * @return                       this builder
         */
        public Builder<T> hideUnknownProperties(boolean hideUnknownProperties) {
            this.hideUnknownProperties = hideUnknownProperties;
            return this;
        }

        /**
         * Sets whether we're printing a template or actual values. For actual values the output may include additional
         * comments, e.g. regarding default values.
         *
         * @param  printTemplate true if we're printing a template, false if we're printing actual property values
         * @return               this builder
         */
        public Builder<T> printTemplate(boolean printTemplate) {
            this.printTemplate = printTemplate;
            return this;
        }

        /**
         * Sets whether to print extra details about property groups.
         *
         * @param  printGroupDetails true to include extra details, false otherwise
         * @return                   this builder
         */
        public Builder<T> printGroupDetails(boolean printGroupDetails) {
            this.printGroupDetails = printGroupDetails;
            return this;
        }

        public SleeperPropertiesPrettyPrinter<T> build() {
            return new SleeperPropertiesPrettyPrinter<>(this);
        }
    }
}
