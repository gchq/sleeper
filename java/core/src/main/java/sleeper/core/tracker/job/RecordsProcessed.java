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
package sleeper.core.tracker.job;

import java.util.Objects;

/**
 * A data structure for storing the number of records read and written.
 */
public class RecordsProcessed {
    private final long recordsRead;
    private final long recordsWritten;

    public RecordsProcessed(long recordsRead, long recordsWritten) {
        this.recordsRead = recordsRead;
        this.recordsWritten = recordsWritten;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordsProcessed that = (RecordsProcessed) o;
        return recordsRead == that.recordsRead && recordsWritten == that.recordsWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsRead, recordsWritten);
    }

    @Override
    public String toString() {
        return "RecordsProcessed{" +
                "recordsRead=" + recordsRead +
                ", recordsWritten=" + recordsWritten +
                '}';
    }
}
