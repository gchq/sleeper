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

import java.util.function.Consumer;

public class AddFileRequest {
    private final String filename;
    private final String partitionId;
    private final FileDataStatistics statistics;

    private AddFileRequest(Builder builder) {
        filename = builder.filename;
        partitionId = builder.partitionId;
        statistics = builder.statistics;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static AddFileRequest addFile(Consumer<Builder> config) {
        Builder builder = builder();
        config.accept(builder);
        return builder.build();
    }

    public FileInfoV2 buildFileInfo() {
        return FileInfoV2.builder()
                .filename(filename)
                .partitionId(partitionId)
                .statistics(statistics)
                .build();
    }

    public static final class Builder {
        private String filename;
        private String partitionId;
        private FileDataStatistics statistics;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder statistics(FileDataStatistics statistics) {
            this.statistics = statistics;
            return this;
        }

        public AddFileRequest build() {
            return new AddFileRequest(this);
        }
    }
}
