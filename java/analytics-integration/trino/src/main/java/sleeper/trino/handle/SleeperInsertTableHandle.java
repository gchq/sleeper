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
package sleeper.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;

/**
 * A handle to support INSERT operations. The handle holds the table that the new rows are to be inserted into and a
 * list of the columns that are to be inserted.
 * <p>
 * Null or default columns are not permitted and so the list of columns that are to be inserted should be the same as
 * the list of columns belonging to the table. If this is always the case, and they always appear in the same order,
 * then it may be possible to remove this field. It would need to be checked carefully.
 */
public class SleeperInsertTableHandle implements ConnectorInsertTableHandle {
    private final SleeperTableHandle sleeperTableHandle;
    private final List<SleeperColumnHandle> sleeperColumnHandlesInOrder;

    @JsonCreator
    public SleeperInsertTableHandle(
            @JsonProperty("sleeperTableHandle") SleeperTableHandle sleeperTableHandle,
            @JsonProperty("sleeperColumnHandlesInOrder") List<SleeperColumnHandle> sleeperColumnHandlesInOrder) {
        this.sleeperTableHandle = sleeperTableHandle;
        this.sleeperColumnHandlesInOrder = sleeperColumnHandlesInOrder;
    }

    @JsonProperty
    public SleeperTableHandle getSleeperTableHandle() {
        return sleeperTableHandle;
    }

    @JsonProperty
    public List<SleeperColumnHandle> getSleeperColumnHandlesInOrder() {
        return sleeperColumnHandlesInOrder;
    }
}
