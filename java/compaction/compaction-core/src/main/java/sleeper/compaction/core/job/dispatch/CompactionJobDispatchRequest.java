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
package sleeper.compaction.core.job.dispatch;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableFilePaths;

import java.time.Instant;
import java.util.Objects;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class CompactionJobDispatchRequest {

    private final String tableId;
    private final String batchKey;
    private final Instant createTime;

    private CompactionJobDispatchRequest(String tableId, String batchKey, Instant createTime) {
        this.tableId = tableId;
        this.batchKey = batchKey;
        this.createTime = createTime;
    }

    public static CompactionJobDispatchRequest forTableWithBatchIdAtTime(
            InstanceProperties instanceProperties, TableProperties tableProperties, String batchId, Instant timeNow) {
        String batchKey = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructCompactionJobBatchPath(batchId);
        return new CompactionJobDispatchRequest(tableProperties.get(TABLE_ID), batchKey, timeNow);
    }

    public String getTableId() {
        return tableId;
    }

    public String getBatchKey() {
        return batchKey;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, batchKey, createTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobDispatchRequest)) {
            return false;
        }
        CompactionJobDispatchRequest other = (CompactionJobDispatchRequest) obj;
        return Objects.equals(tableId, other.tableId) && Objects.equals(batchKey, other.batchKey) && Objects.equals(createTime, other.createTime);
    }

    @Override
    public String toString() {
        return "CompactionJobDispatchRequest{tableId=" + tableId + ", batchKey=" + batchKey + ", createTime=" + createTime + "}";
    }
}
