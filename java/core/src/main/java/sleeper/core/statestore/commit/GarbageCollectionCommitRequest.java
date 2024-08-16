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
package sleeper.core.statestore.commit;

import java.util.List;
import java.util.Objects;

/**
 * A request to commit to the state store when files have been deleted by the garbage collector.
 */
public class GarbageCollectionCommitRequest {

    private final String tableId;
    private final List<String> filenames;

    public GarbageCollectionCommitRequest(String tableId, List<String> filenames) {
        this.tableId = tableId;
        this.filenames = filenames;
    }

    public String getTableId() {
        return tableId;
    }

    public List<String> getFilenames() {
        return filenames;
    }

    @Override
    public String toString() {
        return "GarbageCollectionCommitRequest{tableId=" + tableId + ", filenames=" + filenames + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, filenames);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof GarbageCollectionCommitRequest)) {
            return false;
        }
        GarbageCollectionCommitRequest other = (GarbageCollectionCommitRequest) obj;
        return Objects.equals(tableId, other.tableId) && Objects.equals(filenames, other.filenames);
    }

}
