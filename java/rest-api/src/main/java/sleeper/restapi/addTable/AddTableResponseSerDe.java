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
 * Serialises an AddTable response to and from JSON.
 */
public class AddTableResponseSerDe {

    private final Gson gson;
    private final Gson gsonPrettyPrint;

    public AddTableResponseSerDe() {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
        gsonPrettyPrint = builder.setPrettyPrinting().create();
    }

    /**
     * Serialises an AddTable response to JSON.
     *
     * @param  response the response
     * @return          a JSON representation of the response
     */
    public String toJson(AddTableResponse response) {
        return gson.toJson(response);
    }

    /**
     * Serialises an AddTable response to JSON.
     *
     * @param  response    the response
     * @param  prettyPrint true if the JSON should be formatted for readability
     * @return             a JSON representation of the response
     */
    public String toJson(AddTableResponse response, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrint.toJson(response);
        }
        return toJson(response);
    }

    /**
     * Deserialises a JSON string to an AddTable response.
     *
     * @param  json the JSON string
     * @return      the parsed response
     */
    public AddTableResponse fromJson(String json) {
        AddTableResponse response = gson.fromJson(json, AddTableResponse.class);
        if (response != null) {
            return response.validate();
        } else {
            return null;
        }
    }
}
