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
package sleeper.clients.admin.deploy;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class JarsDiff {

    private final Collection<Path> modifiedAndNew;
    private final Collection<String> s3KeysToDelete;

    private JarsDiff(Collection<Path> modifiedAndNew, Collection<String> s3KeysToDelete) {
        this.modifiedAndNew = modifiedAndNew;
        this.s3KeysToDelete = s3KeysToDelete;
    }

    public static JarsDiff from(Path directory, List<Path> localFiles, Iterable<S3ObjectSummary> s3Objects) throws IOException {
        List<String> deleteKeys = new ArrayList<>();
        Set<Path> uploadJars = new LinkedHashSet<>(localFiles);
        for (S3ObjectSummary object : s3Objects) {
            Path path = directory.resolve(object.getKey());
            if (isUnmodified(uploadJars, path, object)) {
                uploadJars.remove(path);
            } else {
                deleteKeys.add(object.getKey());
            }
        }
        return new JarsDiff(uploadJars, deleteKeys);
    }

    private static boolean isUnmodified(Set<Path> set, Path jar, S3ObjectSummary object) throws IOException {
        if (set.contains(jar)) {
            long fileLastModified = Files.getLastModifiedTime(jar).toInstant().getEpochSecond();
            long bucketLastModified = object.getLastModified().toInstant().getEpochSecond();
            return fileLastModified <= bucketLastModified;
        }
        return false;
    }

    public Collection<Path> getModifiedAndNew() {
        return modifiedAndNew;
    }

    public Collection<String> getS3KeysToDelete() {
        return s3KeysToDelete;
    }
}
