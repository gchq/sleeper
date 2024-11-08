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
package sleeper.compaction.core.job;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;

/**
 * A GSON plugin to serialise/deserialise a compaction job.
 */
public class CompactionJobJsonSerDe implements JsonSerializer<CompactionJob>, JsonDeserializer<CompactionJob> {
    @Override
    public CompactionJob deserialize(JsonElement element, Type type, JsonDeserializationContext context) throws JsonParseException {
        try {
            return CompactionJobSerDe.deserialiseFromString(element.getAsString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonElement serialize(CompactionJob job, Type type, JsonSerializationContext context) {
        try {
            return new JsonPrimitive(CompactionJobSerDe.serialiseToString(job));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
