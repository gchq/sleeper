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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;

public class SyncJars {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncJars.class);
    private final AmazonS3 s3;
    private final Path jarsDirectory;
    private final String bucketName;

    private SyncJars(Builder builder) {
        s3 = builder.s3;
        jarsDirectory = builder.jarsDirectory;
        bucketName = builder.instanceProperties.get(JARS_BUCKET);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void sync() throws IOException {
        LOGGER.info("Creating jars bucket");
        s3.createBucket(bucketName);

        List<Path> jars = ClientUtils.listJarsInDirectory(jarsDirectory);
        LOGGER.info("Found {} jars in local directory", jars.size());

        JarsDiff diff = JarsDiff.from(jarsDirectory, jars, S3Objects.inBucket(s3, bucketName));
        Collection<Path> uploadJars = diff.getModifiedAndNew();
        Collection<String> deleteKeys = diff.getS3KeysToDelete();

        LOGGER.info("Deleting {} jars from bucket", deleteKeys.size());
        if (!deleteKeys.isEmpty()) {
            s3.deleteObjects(new DeleteObjectsRequest(bucketName)
                    .withKeys(deleteKeys.toArray(new String[0])));
        }

        LOGGER.info("Uploading {} jars", uploadJars.size());
        uploadJars.stream().parallel().forEach(jar -> {
            LOGGER.info("Uploading jar: {}", jar.getFileName());
            s3.putObject(bucketName,
                    "" + jar.getFileName(),
                    jar.toFile());
            LOGGER.info("Finished uploading jar: {}", jar.getFileName());
        });
    }

    public static final class Builder {
        private AmazonS3 s3;
        private Path jarsDirectory;
        private InstanceProperties instanceProperties;

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

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public SyncJars build() {
            return new SyncJars(this);
        }
    }
}
