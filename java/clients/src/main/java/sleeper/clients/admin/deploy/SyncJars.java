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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.requireNonEmpty;

public class SyncJars {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncJars.class);
    private final AmazonS3 s3;
    private final Path jarsDirectory;
    private final String bucketName;

    private SyncJars(Builder builder) {
        s3 = requireNonNull(builder.s3, "s3 must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        bucketName = requireNonEmpty(builder.bucketName, "bucketName must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean sync() throws IOException {
        // Note that LocalStack doesn't fail bucket creation if it already exists, but the AWS API does.
        boolean changed = false;
        if (!s3.doesBucketExistV2(bucketName)) {
            LOGGER.info("Creating jars bucket");
            s3.createBucket(bucketName);
            changed = true;
        }

        List<Path> jars = listJarsInDirectory(jarsDirectory);
        LOGGER.info("Found {} jars in local directory", jars.size());

        JarsDiff diff = JarsDiff.from(jarsDirectory, jars, S3Objects.inBucket(s3, bucketName));
        Collection<Path> uploadJars = diff.getModifiedAndNew();
        Collection<String> deleteKeys = diff.getS3KeysToDelete();

        LOGGER.info("Deleting {} jars from bucket", deleteKeys.size());
        if (!deleteKeys.isEmpty()) {
            s3.deleteObjects(new DeleteObjectsRequest(bucketName)
                    .withKeys(deleteKeys.toArray(new String[0])));
            changed = true;
        }

        LOGGER.info("Uploading {} jars", uploadJars.size());
        uploadJars.stream().parallel().forEach(jar -> {
            LOGGER.info("Uploading jar: {}", jar.getFileName());
            s3.putObject(bucketName,
                    "" + jar.getFileName(),
                    jar.toFile());
            LOGGER.info("Finished uploading jar: {}", jar.getFileName());
        });
        if (!uploadJars.isEmpty()) {
            changed = true;
        }
        return changed;
    }

    private static List<Path> listJarsInDirectory(Path directory) throws IOException {
        try (Stream<Path> jars = Files.list(directory)) {
            return jars.filter(path -> path.toFile().getName().endsWith(".jar")).collect(Collectors.toList());
        }
    }

    public static final class Builder {
        private AmazonS3 s3;
        private Path jarsDirectory;
        private String bucketName;

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public SyncJars build() {
            return new SyncJars(this);
        }
    }
}
