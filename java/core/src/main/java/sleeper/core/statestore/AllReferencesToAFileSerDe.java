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
package sleeper.core.statestore;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialises all of a file's references to and from a JSON string.
 */
public class AllReferencesToAFileSerDe {

    private AllReferencesToAFileSerDe() {
    }

    /**
     * Creates GSON configuration to serialise files without update times. Should be combined with
     * {@link FileReferenceSerDe#excludeUpdateTimes}. Used in {@link TransactionSerDe}.
     *
     * @return the GSON configuration
     */
    public static NoUpdateTimesGsonSerDe noUpdateTimes() {
        return new NoUpdateTimesGsonSerDe();
    }

    /**
     * GSON configuration to serialise files without update times. Should be combined with
     * {@link FileReferenceSerDe#excludeUpdateTimes}. Used in {@link TransactionSerDe}.
     */
    public static class NoUpdateTimesGsonSerDe implements JsonSerializer<AllReferencesToAFile>, JsonDeserializer<AllReferencesToAFile> {

        @Override
        public AllReferencesToAFile deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            String filename = object.get("filename").getAsString();
            List<FileReference> references = new ArrayList<>();
            JsonArray referencesArr = object.get("references").getAsJsonArray();
            for (JsonElement referenceElem : referencesArr) {
                JsonObject referenceObj = referenceElem.getAsJsonObject();
                referenceObj.addProperty("filename", filename);
                references.add(context.deserialize(referenceObj, FileReference.class));
            }
            return AllReferencesToAFile.builder()
                    .filename(filename)
                    .references(references)
                    .build();
        }

        @Override
        public JsonElement serialize(AllReferencesToAFile file, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            object.addProperty("filename", file.getFilename());
            JsonArray referencesArr = new JsonArray();
            for (FileReference reference : file.getReferences()) {
                JsonObject referenceObj = context.serialize(reference).getAsJsonObject();
                referenceObj.remove("filename");
                referencesArr.add(referenceObj);
            }
            object.add("references", referencesArr);
            return object;
        }
    }
}
