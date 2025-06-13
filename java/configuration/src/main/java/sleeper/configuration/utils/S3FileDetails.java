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
package sleeper.configuration.utils;

import java.util.Objects;

/** Holder class for import file information. */
public class S3FileDetails {

    private final String filename;
    private final long fileSizeBytes;
    //private final Instant receivedTime;
    //private final String jobId;

    private S3FileDetails(Builder builder) {
        filename = Objects.requireNonNull(builder.filename, "file must not be null");
        fileSizeBytes = builder.fileSizeBytes;
        //tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        //receivedTime = Objects.requireNonNull(builder.receivedTime, "receivedTime must not be null");
        //jobId = builder.jobId;
    }

    public String getFilename() {
        return this.filename;
    }

    /**
     * TODO.
     *
     * @return words.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder. */
    public static final class Builder {
        private String filename;
        private long fileSizeBytes;

        private Builder() {
        }

        /**
         * TODO.
         *
         * @param  filename words
         * @return          words
         */
        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        /**
         * TODO.
         *
         * @param  fileSizeBytes words.
         * @return               words.
         */
        public Builder fileSizeBytes(long fileSizeBytes) {
            this.fileSizeBytes = fileSizeBytes;
            return this;
        }

        public S3FileDetails build() {
            return new S3FileDetails(this);
        }
    }
}
