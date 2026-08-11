/*
 * Copyright 2026 Crown Copyright
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
package sleeper.systemtest.suite.documentation;

import java.lang.annotation.Annotation;
import java.util.Comparator;
import java.util.List;

/**
 * Creates a Markdown table of system tests assigned to parallel suites.
 */
class SystemTestSuiteTable {

    private SystemTestSuiteTable() {
    }

    /**
     * Creates a Markdown table with one column for each supplied suite.
     *
     * @param suites The suite annotations used as table columns
     * @param systemTests The system tests to include
     * @return The Markdown table
     */
    static String create(List<Class<? extends Annotation>> suites, List<Class<?>> systemTests) {
        List<List<String>> testsBySuite = suites.stream()
                .map(suite -> systemTests.stream()
                        .filter(test -> test.isAnnotationPresent(suite))
                        .map(Class::getSimpleName)
                        .sorted()
                        .toList())
                .toList();
        List<Integer> columnWidths = suites.stream()
                .map(suite -> suite.getSimpleName().length())
                .toList();
        columnWidths = calculateColumnWidths(columnWidths, testsBySuite);

        StringBuilder table = new StringBuilder();
        appendRow(table, suites.stream().map(Class::getSimpleName).toList(), columnWidths);
        appendSeparatorRow(table, columnWidths);
        int rowCount = testsBySuite.stream().mapToInt(List::size).max().orElse(0);
        for (int row = 0; row < rowCount; row++) {
            int rowIndex = row;
            List<String> values = testsBySuite.stream()
                    .map(tests -> rowIndex < tests.size() ? tests.get(rowIndex) : "")
                    .toList();
            appendRow(table, removeTrailingEmptyValues(values), columnWidths);
        }
        return table.toString().stripTrailing();
    }

    private static List<Integer> calculateColumnWidths(
            List<Integer> headerWidths, List<List<String>> testsBySuite) {
        return java.util.stream.IntStream.range(0, headerWidths.size())
                .mapToObj(index -> testsBySuite.get(index).stream()
                        .map(String::length)
                        .max(Comparator.naturalOrder())
                        .map(width -> Math.max(width, headerWidths.get(index)))
                        .orElse(headerWidths.get(index)))
                .toList();
    }

    private static void appendRow(StringBuilder table, List<String> values, List<Integer> columnWidths) {
        table.append("|");
        for (int i = 0; i < values.size(); i++) {
            table.append(" ").append(values.get(i));
            table.append(" ".repeat(columnWidths.get(i) - values.get(i).length()));
            table.append(" |");
        }
        table.append("\n");
    }

    private static void appendSeparatorRow(StringBuilder table, List<Integer> columnWidths) {
        table.append("|");
        columnWidths.forEach(width -> table.append("-".repeat(width + 2)).append("|"));
        table.append("\n");
    }

    private static List<String> removeTrailingEmptyValues(List<String> values) {
        int size = values.size();
        while (size > 0 && values.get(size - 1).isEmpty()) {
            size--;
        }
        return values.subList(0, size);
    }
}
