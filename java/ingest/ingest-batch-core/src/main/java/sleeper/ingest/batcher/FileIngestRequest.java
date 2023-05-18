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

public class FileIngestRequest {
    private final String pathToFile;
    private final String tableName;
    private final String jobId;

    private FileIngestRequest(Builder builder) {
        pathToFile = Objects.requireNonNull(builder.pathToFile, "pathToFile must not be null");
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        jobId = builder.jobId;
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

    public String getJobId() {
        return jobId;
    }

    public Builder toBuilder() {
        return builder().pathToFile(pathToFile)
                .tableName(tableName)
                .jobId(jobId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileIngestRequest that = (FileIngestRequest) o;

        if (!pathToFile.equals(that.pathToFile)) {
            return false;
        }
        if (!tableName.equals(that.tableName)) {
            return false;
        }
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        int result = pathToFile.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FileIngestRequest{" +
                "pathToFile='" + pathToFile + '\'' +
                ", tableName='" + tableName + '\'' +
                ", jobId='" + jobId + '\'' +
                '}';
    }

    public static final class Builder {
        private String pathToFile;
        private String tableName;
        private String jobId;

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

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public FileIngestRequest build() {
            return new FileIngestRequest(this);
        }
    }
}
