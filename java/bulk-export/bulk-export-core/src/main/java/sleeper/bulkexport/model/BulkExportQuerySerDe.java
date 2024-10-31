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
package sleeper.bulkexport.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serialises a query to and from JSON.
 */
public class BulkExportQuerySerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    public BulkExportQuerySerDe() {
        GsonBuilder builder = new GsonBuilder()
                .serializeNulls();
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
    }

    public String toJson(BulkExportQuery query) {
        return gson.toJson(BulkExportQueryJson.from(query));
    }

    public String toJson(BulkExportQuery query, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(BulkExportQueryJson.from(query));
        }
        return toJson(query);
    }

    public BulkExportQuery fromJson(String json) {
        BulkExportQueryJson queryJson = gson.fromJson(json, BulkExportQueryJson.class);
        return BulkExportQueryJson.to(queryJson);
    }
}
