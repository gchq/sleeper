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
package sleeper.compaction.job.status;

import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobCommittedStatus implements ProcessStatusUpdate {

    private final Instant updateTime;

    private CompactionJobCommittedStatus(Instant updateTime) {
        this.updateTime = updateTime;
    }

    public static CompactionJobCommittedStatus committedAt(Instant updateTime) {
        return new CompactionJobCommittedStatus(updateTime);
    }

    @Override
    public boolean isPartOfRun() {
        return true;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobCommittedStatus)) {
            return false;
        }
        CompactionJobCommittedStatus other = (CompactionJobCommittedStatus) obj;
        return Objects.equals(updateTime, other.updateTime);
    }

    @Override
    public String toString() {
        return "CompactionJobCommittedStatus{updateTime=" + updateTime + "}";
    }

}
