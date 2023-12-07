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

package sleeper.core.statestore;

import java.util.List;

public class AssignJobToFilesRequest {

    private final String tableId;
    private final String jobId;
    private final String partitionId;
    private final List<String> files;

    private AssignJobToFilesRequest(Builder builder) {
        tableId = builder.tableId;
        jobId = builder.jobId;
        partitionId = builder.partitionId;
        files = builder.files;
    }

    public interface Client {
        void updateJobStatusOfFiles(List<AssignJobToFilesRequest> jobs) throws StateStoreException;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableId() {
        return tableId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public List<String> getFiles() {
        return files;
    }

    public static final class Builder {
        private String tableId;
        private String jobId;
        private String partitionId;
        private List<String> files;

        private Builder() {
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public AssignJobToFilesRequest build() {
            return new AssignJobToFilesRequest(this);
        }
    }
}
