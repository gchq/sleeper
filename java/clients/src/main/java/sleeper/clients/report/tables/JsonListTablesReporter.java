/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.report.tables;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import sleeper.clients.util.ClientsGsonConfig;
import sleeper.core.table.TableStatus;

import java.io.PrintStream;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Returns tables in JSON format to the user on the console.
 */
public class JsonListTablesReporter implements ListTablesReporter {

    private final Gson gson = ClientsGsonConfig.standardBuilder().create();
    private final PrintStream out;

    public JsonListTablesReporter() {
        this(System.out);
    }

    public JsonListTablesReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(Stream<TableStatus> tables) {
        JsonArray tablesArray = tables.map(t -> {
            JsonObject tableObj = new JsonObject();
            tableObj.addProperty("name", t.getTableName());
            tableObj.addProperty("id", t.getTableUniqueId());
            return tableObj;
        }).collect(Collector.of(JsonArray::new, JsonArray::add, (left, right) -> {
            left.addAll(right);
            return left;
        }));

        JsonObject tablesObj = new JsonObject();
        tablesObj.add("tables", tablesArray);
        out.println(gson.toJson(tablesObj));
    }
}
