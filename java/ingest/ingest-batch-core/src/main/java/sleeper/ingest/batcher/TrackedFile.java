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

package sleeper.ingest.batcher;

import java.util.Objects;

public class TrackedFile {
    private final String pathToFile;

    private TrackedFile(Builder builder) {
        pathToFile = Objects.requireNonNull(builder.pathToFile, "pathToFile must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrackedFile that = (TrackedFile) o;
        return Objects.equals(pathToFile, that.pathToFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathToFile);
    }

    @Override
    public String toString() {
        return "TrackedFile{" +
                "pathToFile='" + pathToFile + '\'' +
                '}';
    }

    public static final class Builder {
        private String pathToFile;

        public Builder() {
        }

        public Builder pathToFile(String pathToFile) {
            this.pathToFile = pathToFile;
            return this;
        }

        public TrackedFile build() {
            return new TrackedFile(this);
        }
    }
}
