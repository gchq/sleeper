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

public class TrackedFile {
    private final String pathToFile;
    private final String tableName;

    private TrackedFile(Builder builder) {
        pathToFile = builder.pathToFile;
        tableName = builder.tableName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getPathToFile() {
        return pathToFile;
    }

    public String getTableName() {
        return tableName;
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

        if (!pathToFile.equals(that.pathToFile)) {
            return false;
        }
        return tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        int result = pathToFile.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TrackedFile{" +
                "pathToFile='" + pathToFile + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }

    public static final class Builder {
        private String pathToFile;
        private String tableName;

        private Builder() {
        }

        public Builder pathToFile(String pathToFile) {
            this.pathToFile = pathToFile;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public TrackedFile build() {
            return new TrackedFile(this);
        }
    }
}
