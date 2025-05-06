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
package sleeper.clients.deploy.jar;

import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class JarsDiff {

    private final Collection<Path> modifiedAndNew;
    private final Collection<String> s3KeysToDelete;

    private JarsDiff(Collection<Path> modifiedAndNew, Collection<String> s3KeysToDelete) {
        this.modifiedAndNew = modifiedAndNew;
        this.s3KeysToDelete = s3KeysToDelete;
    }

    public static JarsDiff from(Path directory, List<Path> localFiles, ListObjectsV2Iterable listObjects) throws IOException {
        return from(directory, localFiles, listObjects.stream()
                .flatMap(response -> response.contents().stream())
                .map(object -> new S3KeyAndModifiedTime(object.key(), object.lastModified())));
    }

    private static JarsDiff from(Path directory, List<Path> localFiles, Stream<S3KeyAndModifiedTime> s3Objects) throws IOException {
        List<String> deleteKeys = new ArrayList<>();
        Set<Path> uploadJars = new LinkedHashSet<>(localFiles);
        for (S3KeyAndModifiedTime object : (Iterable<? extends S3KeyAndModifiedTime>) s3Objects::iterator) {
            Path path = directory.resolve(object.key);
            if (isUnmodified(uploadJars, path, object)) {
                uploadJars.remove(path);
            } else {
                deleteKeys.add(object.key);
            }
        }
        return new JarsDiff(uploadJars, deleteKeys);
    }

    private static boolean isUnmodified(Set<Path> set, Path jar, S3KeyAndModifiedTime object) throws IOException {
        if (set.contains(jar)) {
            long fileLastModified = Files.getLastModifiedTime(jar).toInstant().getEpochSecond();
            long bucketLastModified = object.lastModifiedTime.getEpochSecond();
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

    private static class S3KeyAndModifiedTime {
        private final String key;
        private final Instant lastModifiedTime;

        private S3KeyAndModifiedTime(String key, Instant lastModifiedTime) {
            this.key = key;
            this.lastModifiedTime = lastModifiedTime;
        }
    }
}
