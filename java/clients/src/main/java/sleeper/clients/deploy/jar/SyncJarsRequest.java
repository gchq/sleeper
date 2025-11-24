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

import sleeper.core.properties.instance.InstanceProperties;

import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Predicate;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

public class SyncJarsRequest {

    private final String bucketName;
    private final Predicate<Path> uploadFilter;
    private final boolean deleteOldJars;

    private SyncJarsRequest(Builder builder) {
        bucketName = Objects.requireNonNull(builder.bucketName, "bucketName must not be null");
        uploadFilter = Objects.requireNonNull(builder.uploadFilter, "uploadFilter must not be null");
        deleteOldJars = builder.deleteOldJars;
    }

    public static SyncJarsRequest from(InstanceProperties instanceProperties) {
        return builder().instanceProperties(instanceProperties).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getBucketName() {
        return bucketName;
    }

    public Predicate<Path> getUploadFilter() {
        return uploadFilter;
    }

    public boolean isDeleteOldJars() {
        return deleteOldJars;
    }

    public static class Builder {
        private String bucketName;
        private Predicate<Path> uploadFilter = jar -> true;
        private boolean deleteOldJars = false;

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder uploadFilter(Predicate<Path> uploadFilter) {
            this.uploadFilter = uploadFilter;
            return this;
        }

        public Builder deleteOldJars(boolean deleteOldJars) {
            this.deleteOldJars = deleteOldJars;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            return bucketName(instanceProperties.get(JARS_BUCKET));
        }

        public SyncJarsRequest build() {
            return new SyncJarsRequest(this);
        }

    }

}
