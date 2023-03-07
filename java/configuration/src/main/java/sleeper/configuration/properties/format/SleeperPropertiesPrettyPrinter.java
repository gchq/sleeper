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

import org.apache.commons.lang.WordUtils;

import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SleeperPropertiesPrettyPrinter<T extends SleeperProperty> {

    private final List<T> sortedProperties;
    private final Consumer<String> printLine;

    public SleeperPropertiesPrettyPrinter(List<T> properties, List<PropertyGroup> groups, Consumer<String> printLine) {
        this.sortedProperties = PropertyGroup.sortPropertiesByGroup(properties, groups);
        this.printLine = printLine;
    }

    private void println(String line) {
        printLine.accept(line);
    }

    private void println() {
        printLine.accept("");
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
            String value = properties.get(property);
            if (value != null) {
                println(property.getPropertyName() + "=" + value);
            } else {
                println("# (no value set, uncomment to set a value)");
                println("# " + property.getPropertyName() + "=");
            }
        }
    }

    private static String formatDescription(SleeperProperty property) {
        return formatString(property.getDescription());
    }

    private static String formatDescription(PropertyGroup group) {
        return formatString(group.getDescription());
    }

    private static String formatString(String str) {
        return Arrays.stream(str.split("\n")).
                map(line -> "# " + WordUtils.wrap(line, 100).replace("\n", "\n# "))
                .collect(Collectors.joining("\n"));
    }
}
