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

import java.time.Instant;

public class FileReferenceCount {

    private final String filename;
    private final int references;
    private final Instant lastUpdateTime;

    private FileReferenceCount(Builder builder) {
        filename = builder.filename;
        references = builder.references;
        lastUpdateTime = builder.lastUpdateTime;
    }

    public static FileReferenceCount empty(String filename) {
        return builder().filename(filename).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public FileReferenceCount increment(Instant updateTime) {
        return builder().filename(filename).lastUpdateTime(updateTime).references(references + 1).build();
    }

    public FileReferenceCount decrement(Instant updateTime) {
        return builder().filename(filename).lastUpdateTime(updateTime).references(references - 1).build();
    }

    public String getFilename() {
        return filename;
    }

    public int getReferences() {
        return references;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public static final class Builder {
        private String filename;
        private int references;
        private Instant lastUpdateTime;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder references(int references) {
            this.references = references;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public FileReferenceCount build() {
            return new FileReferenceCount(this);
        }
    }
}
