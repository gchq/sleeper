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
package sleeper.build.status;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class WorkflowStatus {

    private final ChunkStatuses chunks;
    private final List<String> chunkIdsToBuild;

    public WorkflowStatus(ChunkStatuses chunks, List<String> chunkIdsToBuild) {
        this.chunks = Objects.requireNonNull(chunks, "chunks must not be null");
        this.chunkIdsToBuild = Collections.unmodifiableList(
                Objects.requireNonNull(chunkIdsToBuild, "chunkIdsToBuild must not be null"));
    }

    public boolean hasPreviousFailures() {
        return chunks.isFailCheck();
    }

    public List<String> previousBuildsReportLines() throws UnsupportedEncodingException {
        return chunks.reportLines();
    }

    public List<String> chunkIdsToBuild() {
        return chunkIdsToBuild;
    }
}
