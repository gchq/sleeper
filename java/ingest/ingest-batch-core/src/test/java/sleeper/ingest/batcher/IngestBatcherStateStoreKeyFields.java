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

public class IngestBatcherStateStoreKeyFields {

    private final String pathToFile;
    private final String tableName;
    private final String jobId;

    public IngestBatcherStateStoreKeyFields(FileIngestRequest request) {
        pathToFile = request.getPathToFile();
        tableName = request.getTableName();
        jobId = null;
    }

    public IngestBatcherStateStoreKeyFields(FileIngestRequest request, String jobId) {
        this.pathToFile = request.getPathToFile();
        this.tableName = request.getTableName();
        this.jobId = jobId;
    }

    public boolean isAssignedToJob() {
        return jobId != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IngestBatcherStateStoreKeyFields that = (IngestBatcherStateStoreKeyFields) o;

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
        return "IngestBatcherStateStoreKeyFields{" +
                "filePath='" + pathToFile + '\'' +
                ", tableName='" + tableName + '\'' +
                ", jobId='" + jobId + '\'' +
                '}';
    }
}
