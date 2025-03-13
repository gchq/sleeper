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
 * Serialises a bulk export query to and from JSON.
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
     * Formats a bulk export query as a JSON string.
     *
     * @param  query the query
     * @return       a JSON string representation of the query
     */
    public String toJson(BulkExportQuery query) {
        return gson.toJson(query);
    }

    /**
     * Formats a bulk export query as a JSON string with the option to pretty print.
     *
     * @param  query       to query
     * @param  prettyPrint option to pretty print
     * @return             a JSON string representation of the query
     */
    public String toJson(BulkExportQuery query, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(query);
        }
        return toJson(query);
    }

    /**
     * Parses a JSON string as a bulk export query.
     *
     * @param  json the JSON string
     * @return      the parsed query
     */
    public BulkExportQuery fromJson(String json) {
        BulkExportQuery query = gson.fromJson(json, BulkExportQuery.class);
        return query.validate();
    }
}
