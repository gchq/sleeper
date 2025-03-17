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

package sleeper.core.util;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.time.Instant;

/**
 * A helper class for common GSON configuration for JSON internal to Sleeper.
 */
public class GsonConfig {

    private GsonConfig() {
    }

    /**
     * Creates a GSON builder preconfigured for JSON internal to Sleeper. This must be further configured to work with
     * specific Sleeper entities.
     *
     * @return the new builder
     */
    public static GsonBuilder standardBuilder() {
        return new GsonBuilder().serializeSpecialFloatingPointValues()
                .registerTypeAdapter(Instant.class, new InstantSerDe());
    }

    /**
     * A GSON plugin to serialise/deserialise an instant.
     */
    private static class InstantSerDe implements JsonSerializer<Instant>, JsonDeserializer<Instant> {
        @Override
        public Instant deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            return Instant.ofEpochMilli(element.getAsLong());
        }

        @Override
        public JsonElement serialize(Instant instant, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(instant.toEpochMilli());
        }
    }
}
