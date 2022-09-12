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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class ChunksStatus {

    private final List<ChunkStatus> chunks;

    private ChunksStatus(Builder builder) {
        chunks = Objects.requireNonNull(builder.chunks, "chunks must not be null");
    }

    public static ChunksStatus from(Properties properties) {
        return chunks(chunksFrom(properties));
    }

    public boolean isFailCheck() {
        return chunks.stream().anyMatch(ChunkStatus::isFailCheck);
    }

    public void report(PrintStream out) {
        chunks.forEach(c -> c.report(out));
    }

    public String reportString() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        report(new PrintStream(os));
        return os.toString();
    }

    public static ChunksStatus chunks(ChunkStatus... chunks) {
        return builder().chunks(chunks).build();
    }

    public static ChunksStatus chunks(List<ChunkStatus> chunks) {
        return builder().chunks(chunks).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChunksStatus that = (ChunksStatus) o;
        return chunks.equals(that.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunks);
    }

    @Override
    public String toString() {
        return "ChunksStatus{" +
                "chunks=" + chunks +
                '}';
    }

    public static final class Builder {
        private List<ChunkStatus> chunks;

        private Builder() {
        }

        public Builder chunks(List<ChunkStatus> chunks) {
            this.chunks = chunks;
            return this;
        }

        public Builder chunks(ChunkStatus... chunks) {
            return chunks(Arrays.asList(chunks));
        }

        public ChunksStatus build() {
            return new ChunksStatus(this);
        }
    }

    private static List<ChunkStatus> chunksFrom(Properties properties) {
        String[] chunkNames = properties.getProperty("chunks").split(",");
        List<ChunkStatus> chunks = new ArrayList<>(chunkNames.length);
        for (String chunkName : chunkNames) {
            chunks.add(ChunkStatus.chunk(chunkName)
                    .status(properties.getProperty(chunkName + ".status"))
                    .conclusion(properties.getProperty(chunkName + ".conclusion"))
                    .build());
        }
        return chunks;
    }
}
