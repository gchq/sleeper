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
package sleeper.restapi.addTable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serialises an AddTable request to and from JSON.
 */
public class AddTableRequestSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public AddTableRequestSerDe() {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises an AddTable request to JSON.
     *
     * @param  request the request
     * @return         a JSON representation of the request
     */
    public String toJson(AddTableRequest request) {
        return gson.toJson(request);
    }

    /**
     * Serialises an AddTable request to JSON.
     *
     * @param  request     the request
     * @param  prettyPrint true if the JSON should be formatted for readability
     * @return             a JSON representation of the request
     */
    public String toJson(AddTableRequest request, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrint.toJson(request);
        }
        return toJson(request);
    }

    /**
     * Deserialises a JSON string to an AddTable request.
     *
     * @param  json the JSON string
     * @return      the parsed request
     */
    public AddTableRequest fromJson(String json) {
        AddTableRequest request = gson.fromJson(json, AddTableRequest.class);
        if (request != null) {
            return request.validate();
        } else {
            return null;
        }
    }
}
