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

import sleeper.core.util.GsonConfig;

import java.util.Collection;
import java.util.List;

public class FileReferenceSerDe {
    private final Gson gson = GsonConfig.standardBuilder().create();

    public String toJson(FileReference file) {
        return gson.toJson(file);
    }

    public FileReference fromJson(String json) {
        return gson.fromJson(json, FileReference.class);
    }

    public String collectionToJson(Collection<FileReference> files) {
        JsonArray array = new JsonArray();
        files.forEach(reference -> array.add(gson.toJsonTree(reference)));
        return gson.toJson(array);
    }

    public List<FileReference> listFromJson(String json) {
        return gson.fromJson(json, new ListType());
    }

    private static class ListType extends TypeToken<List<FileReference>> {
    }

    public static ExclusionStrategy excludeUpdateTimes() {
        return new ExcludeUpdateTimes();
    }

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
