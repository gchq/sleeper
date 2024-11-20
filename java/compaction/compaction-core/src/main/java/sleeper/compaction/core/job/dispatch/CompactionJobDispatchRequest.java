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

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_TIMEOUT_SECS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class CompactionJobDispatchRequest {

    private final String tableId;
    private final String batchKey;
    private final Instant expiryTime;

    private CompactionJobDispatchRequest(String tableId, String batchKey, Instant expiryTime) {
        this.tableId = tableId;
        this.batchKey = batchKey;
        this.expiryTime = expiryTime;
    }

    public static CompactionJobDispatchRequest forTableWithBatchIdAtTime(
            InstanceProperties instanceProperties, TableProperties tableProperties, String batchId, Instant timeNow) {
        String batchKey = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructCompactionJobBatchPath(batchId);
        Duration sendTimeout = Duration.ofSeconds(tableProperties.getInt(COMPACTION_JOB_SEND_TIMEOUT_SECS));
        return new CompactionJobDispatchRequest(tableProperties.get(TABLE_ID), batchKey, timeNow.plus(sendTimeout));
    }

    public String getTableId() {
        return tableId;
    }

    public String getBatchKey() {
        return batchKey;
    }

    public Instant getExpiryTime() {
        return expiryTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, batchKey, expiryTime);
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
        return Objects.equals(tableId, other.tableId) && Objects.equals(batchKey, other.batchKey) && Objects.equals(expiryTime, other.expiryTime);
    }

    @Override
    public String toString() {
        return "CompactionJobDispatchRequest{tableId=" + tableId + ", batchKey=" + batchKey + ", expiryTime=" + expiryTime + "}";
    }
}