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

package sleeper.core.testutils.printers;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TablesPrinter {

    private TablesPrinter() {
    }

    public static String printForAllTables(Collection<String> tableNames, Function<String, String> tablePrinter) {
        Map<String, List<String>> tableNamesByPrintedValue = tableNames.stream()
                .collect(Collectors.groupingBy(tablePrinter));
        List<Map.Entry<String, List<String>>> printedSortedByFrequency = tableNamesByPrintedValue.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getValue().size()))
                .collect(Collectors.toUnmodifiableList());
        ToStringPrintStream printer = new ToStringPrintStream();
        PrintStream out = printer.getPrintStream();

        for (Map.Entry<String, List<String>> entry : printedSortedByFrequency) {
            String printed = entry.getKey();
            List<String> printedForTables = entry.getValue();
            int frequency = printedForTables.size();
            if (frequency == 1) {
                if (printedSortedByFrequency.size() == 1) {
                    out.println("One table");
                } else {
                    out.println("Different for one table");
                }
            } else {
                out.println("Same for " + frequency + " tables");
            }
            out.println(printed);
        }
        return printer.toString();
    }
}
