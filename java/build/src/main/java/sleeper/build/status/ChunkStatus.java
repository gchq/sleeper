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
package sleeper.build.status;

import java.util.Objects;

public class ChunkStatus {

    private final String chunk;
    private final String status;
    private final String conclusion;

    private ChunkStatus(Builder builder) {
        chunk = Objects.requireNonNull(ignoreEmpty(builder.chunk), "chunk must not be null");
        status = Objects.requireNonNull(ignoreEmpty(builder.status), "status must not be null");
        conclusion = ignoreEmpty(builder.conclusion);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder chunk(String chunk) {
        return builder().chunk(chunk);
    }

    public static ChunkStatus success(String chunk) {
        return chunk(chunk).status("completed").conclusion("success").build();
    }

    public static ChunkStatus inProgress(String chunk) {
        return chunk(chunk).status("in_progress").build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChunkStatus that = (ChunkStatus) o;
        return chunk.equals(that.chunk) && status.equals(that.status) && Objects.equals(conclusion, that.conclusion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunk, status, conclusion);
    }

    @Override
    public String toString() {
        return "ChunkStatus{" +
                "chunk='" + chunk + '\'' +
                ", status='" + status + '\'' +
                ", conclusion='" + conclusion + '\'' +
                '}';
    }

    public static final class Builder {
        private String chunk;
        private String status;
        private String conclusion;

        private Builder() {
        }

        public Builder chunk(String chunk) {
            this.chunk = chunk;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder conclusion(String conclusion) {
            this.conclusion = conclusion;
            return this;
        }

        public ChunkStatus build() {
            return new ChunkStatus(this);
        }
    }

    private static String ignoreEmpty(String string) {
        if (string == null || string.isEmpty()) {
            return null;
        } else {
            return string;
        }
    }
}
