/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.ingest.batcher.core.testutil;

import sleeper.ingest.batcher.core.FileIngestRequest;

import java.util.Objects;

public class IngestBatcherStoreKeyFields {

    private final String file;
    private final String tableId;
    private final String jobId;

    private IngestBatcherStoreKeyFields(FileIngestRequest request) {
        file = request.getFile();
        tableId = request.getTableId();
        jobId = request.getJobId();
    }

    public static IngestBatcherStoreKeyFields keyFor(FileIngestRequest request) {
        return new IngestBatcherStoreKeyFields(request);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IngestBatcherStoreKeyFields that = (IngestBatcherStoreKeyFields) o;

        if (!file.equals(that.file)) {
            return false;
        }
        if (!tableId.equals(that.tableId)) {
            return false;
        }
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + tableId.hashCode();
        result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IngestBatcherStoreKeyFields{" +
                "filePath='" + file + '\'' +
                ", tableId='" + tableId + '\'' +
                ", jobId='" + jobId + '\'' +
                '}';
    }
}
