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

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.reflect.TypeToken;

import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;
import sleeper.core.util.GsonConfig;

import java.util.Collection;
import java.util.List;

/**
 * Serialises a file reference to and from a JSON string.
 */
public class FileReferenceSerDe {
    private final Gson gson = GsonConfig.standardBuilder().create();

    /**
     * Serialises a file reference to JSON.
     *
     * @param  file the file reference
     * @return      the JSON string
     */
    public String toJson(FileReference file) {
        return gson.toJson(file);
    }

    /**
     * Deserialises a file reference from JSON.
     *
     * @param  json the JSON string
     * @return      the file reference
     */
    public FileReference fromJson(String json) {
        return gson.fromJson(json, FileReference.class);
    }

    /**
     * Serialises file references as a JSON array.
     *
     * @param  files the file references
     * @return       the JSON string
     */
    public String collectionToJson(Collection<FileReference> files) {
        JsonArray array = new JsonArray();
        files.forEach(reference -> array.add(gson.toJsonTree(reference)));
        return gson.toJson(array);
    }

    /**
     * Deserialises file references from a JSON array.
     *
     * @param  json the JSON string
     * @return      the file references
     */
    public List<FileReference> listFromJson(String json) {
        return gson.fromJson(json, new ListType());
    }

    /**
     * A GSON type token for a list of file references.
     */
    private static class ListType extends TypeToken<List<FileReference>> {
    }

    /**
     * Creates GSON configuration to exclude update times when serialising file references. This is used with
     * {@link AllReferencesToAFileSerDe#noUpdateTimes} in {@link TransactionSerDe}.
     *
     * @return the GSON configuration
     */
    public static ExclusionStrategy excludeUpdateTimes() {
        return new ExcludeUpdateTimes();
    }

    /**
     * GSON configuration to exclude update times when serialising file references. This is used with
     * {@link AllReferencesToAFileSerDe#noUpdateTimes} in {@link TransactionSerDe}.
     */
    private static class ExcludeUpdateTimes implements ExclusionStrategy {

        @Override
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getDeclaringClass() == FileReference.class
                    && f.getName().equals("lastStateStoreUpdateTime");
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }

    }
}
