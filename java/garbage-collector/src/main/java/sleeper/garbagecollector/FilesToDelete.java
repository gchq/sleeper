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
package sleeper.garbagecollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class FilesToDelete {

    private final Map<String, List<String>> bucketNameToObjectKey;
    private final Map<String, String> objectKeyToFilename;

    private FilesToDelete(Map<String, List<String>> bucketNameToObjectKey, Map<String, String> objectKeyToFilename) {
        this.bucketNameToObjectKey = bucketNameToObjectKey;
        this.objectKeyToFilename = objectKeyToFilename;
    }

    public static FilesToDelete from(List<String> filenames) {
        Map<String, List<String>> bucketNameToObjectKey = new HashMap<>();
        Map<String, String> objectKeyToFilename = new HashMap<>();
        for (String filename : filenames) {
            FileToDelete file = FileToDelete.fromFilename(filename);
            List<String> objectKeys = bucketNameToObjectKey.computeIfAbsent(file.bucketName(), name -> new ArrayList<>());
            file.streamObjectKeys().forEach(objectKey -> {
                objectKeys.add(objectKey);
                objectKeyToFilename.put(objectKey, filename);
            });
        }
        return new FilesToDelete(bucketNameToObjectKey, objectKeyToFilename);
    }

    public void forEachBucketObjectKeys(BiConsumer<String, List<String>> consumer) {
        bucketNameToObjectKey.forEach(consumer);
    }

    public String getFilenameForObjectKey(String objectKey) {
        return objectKeyToFilename.get(objectKey);
    }

}
