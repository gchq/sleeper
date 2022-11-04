/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.job;

import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.List;

public class IngestJobRunnerResult {
    private final List<Path> pathList;
    private final long numRecordsWritten;

    private IngestJobRunnerResult(Builder builder) {
        pathList = builder.pathList;
        numRecordsWritten = builder.numRecordsWritten;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Path> getPathList() {
        return Collections.unmodifiableList(pathList);
    }

    public long getNumRecordsWritten() {
        return numRecordsWritten;
    }

    public static final class Builder {
        private List<Path> pathList;
        private long numRecordsWritten;

        private Builder() {
        }

        public Builder pathList(List<Path> pathList) {
            this.pathList = pathList;
            return this;
        }

        public Builder numRecordsWritten(long numRecordsWritten) {
            this.numRecordsWritten = numRecordsWritten;
            return this;
        }

        public IngestJobRunnerResult build() {
            return new IngestJobRunnerResult(this);
        }
    }
}
