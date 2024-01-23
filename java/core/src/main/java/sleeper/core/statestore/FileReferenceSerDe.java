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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.util.Collection;
import java.util.List;

public class FileReferenceSerDe {
    private final Gson gson = new GsonBuilder().create();

    public String toJson(FileReference file) {
        return gson.toJson(file);
    }

    public FileReference fromJson(String json) {
        return gson.fromJson(json, FileReference.class);
    }

    public String collectionToJson(Collection<FileReference> files) {
        return gson.toJson(files);
    }

    public List<FileReference> listFromJson(String json) {
        return gson.fromJson(json, new ListType());
    }

    private static class ListType extends TypeToken<List<FileReference>> {
    }
}
