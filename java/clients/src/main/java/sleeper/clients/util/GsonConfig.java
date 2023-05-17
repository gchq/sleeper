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
package sleeper.clients.util;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;

public class GsonConfig {

    private GsonConfig() {
    }

    public static GsonBuilder standardBuilder() {
        return new GsonBuilder().serializeSpecialFloatingPointValues()
                .registerTypeAdapter(Instant.class, new InstantSerDe())
                .registerTypeAdapter(Duration.class, durationSerializer())
                .setPrettyPrinting();
    }

    private static JsonSerializer<Duration> durationSerializer() {
        return (duration, type, context) -> new JsonPrimitive(duration.toMillis() / 1000.0);
    }

    private static class InstantSerDe implements JsonSerializer<Instant>, JsonDeserializer<Instant> {
        @Override
        public Instant deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
            return Instant.parse(element.getAsString());
        }

        @Override
        public JsonElement serialize(Instant instant, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(instant.toString());
        }
    }

}
