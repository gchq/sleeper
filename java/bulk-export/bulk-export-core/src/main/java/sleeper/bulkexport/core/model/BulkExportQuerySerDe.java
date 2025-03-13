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
package sleeper.bulkexport.core.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serialises an BulkExportQuery to and from JSON.
 */
public class BulkExportQuerySerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public BulkExportQuerySerDe() {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
    }

    /**
     * Formats a BulkExportQuery as a JSON string.
     *
     * @param  query to format
     *
     * @return       a JSON string of the query
     */
    public String toJson(BulkExportQuery query) {
        return gson.toJson(query);
    }

    /**
     * Formats a BulkExportQuery as a JSON string with the option to pretty
     * print.
     *
     * @param  query       to format
     * @param  prettyPrint option to pretty print
     *
     * @return             a JSON string of the query
     */
    public String toJson(BulkExportQuery query, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(query);
        }
        return toJson(query);
    }

    /**
     * Formats a JSON string to a BulkExportQuery object.
     *
     * @param  json The JSON string to format.
     *
     * @return      The parsed object as BulkExportQuery.
     */
    public BulkExportQuery fromJson(String json) {
        BulkExportQuery query = gson.fromJson(json, BulkExportQuery.class);
        return query.validate();
    }
}
